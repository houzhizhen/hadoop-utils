package org.apache.hadoop.hdfs.server.azmover;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferBlockTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TransferBlockTask.class);

  private final NameNodeConnector nnc;
  private final HdfsLocatedFileStatus fileStatus;
  private final DatanodeInfo source;
  private final DatanodeInfo target;
  private final DatanodeInfo deletion;
  private final ExtendedBlock extendedBlock;
  private final long blockMoveTimeout;
  private final AzCluster cluster;

  public TransferBlockTask(
      NameNodeConnector nnc,
      HdfsLocatedFileStatus fileStatus,
      DatanodeInfo source,
      DatanodeInfo target,
      @Nullable DatanodeInfo deletion,
      ExtendedBlock extendedBlock,
      long blockMoveTimeout,
      AzCluster cluster) {
    this.nnc = nnc;
    this.fileStatus = fileStatus;
    this.source = Objects.requireNonNull(source, "Source can not be null");
    this.target = Objects.requireNonNull(target, "Target can not be null");
    this.deletion = deletion;
    this.blockMoveTimeout = blockMoveTimeout;
    if (source.getDatanodeUuid().equals(target.getDatanodeUuid())) {
      throw new IllegalStateException("Source equals target");
    }
    this.extendedBlock = extendedBlock;
    this.cluster = cluster;
  }

  @Override
  public void run() {
    try {
      replaceBlock();
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Can not replace block, source = %s, target = %s", source.getHostName(),
              target.getHostName()), e);
    }
  }

  private void replaceBlock() throws IOException {
    Socket sock = new Socket();
    DataOutputStream out = null;
    DataInputStream in = null;
    Configuration conf = nnc.getDistributedFileSystem().getConf();
    try {
      sock.connect(NetUtils.createSocketAddr(target.getXferAddr(false)),
          HdfsConstants.READ_TIMEOUT);
      sock.setSoTimeout(HdfsConstants.READ_TIMEOUT * 5);
      sock.setKeepAlive(true);
      OutputStream unbufOut = sock.getOutputStream();
      InputStream unbufIn = sock.getInputStream();
      KeyManager km = nnc.getKeyManager();
      Token<BlockTokenIdentifier> accessToken = km.getAccessToken(extendedBlock,
          new StorageType[]{StorageType.DISK}, new String[0]);
      SaslDataTransferClient saslClient = new SaslDataTransferClient(conf,
          DataTransferSaslUtil.getSaslPropertiesResolver(conf),
          TrustedChannelResolver.getInstance(conf), new AtomicBoolean(false));
      IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, km, accessToken, target);
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut, 4096));
      in = new DataInputStream(new BufferedInputStream(unbufIn, 4096));
      LOG.info(
          "Changing block distribution: blockId = blk_{}, source = {}, target = {}, delete = {}, path = {}",
          extendedBlock.getBlockId(),
          source.getHostName(),
          target.getHostName(),
          deletion == null ? null : deletion.getHostName(),
          fileStatus.getPath());
      new Sender(out).replaceBlock(extendedBlock, StorageType.DISK, accessToken,
          deletion == null ? "" : deletion.getDatanodeUuid(), source, null);
      receiveResponse(in);
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      IOUtils.closeSocket(sock);
    }
  }

  private void receiveResponse(DataInputStream in) throws IOException {
    long startTime = System.currentTimeMillis();
    BlockOpResponseProto response =
        BlockOpResponseProto.parseFrom(vintPrefixed(in));
    while (response.getStatus() == Status.IN_PROGRESS) {
      // read intermediate responses
      response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      // Stop waiting for slow block moves. Even if it stops waiting,
      // the actual move may continue.
      if (System.currentTimeMillis() - startTime > blockMoveTimeout) {
        throw new IOException("Block move timed out");
      }
    }
    String logInfo = "Reported Block move is failed";
    DataTransferProtoUtil.checkBlockOpStatus(response, logInfo, true);
  }
}

