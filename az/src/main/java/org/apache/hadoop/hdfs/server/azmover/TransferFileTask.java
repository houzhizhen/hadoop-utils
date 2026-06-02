package org.apache.hadoop.hdfs.server.azmover;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzPolicyProvider;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferFileTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TransferFileTask.class);

  private final HdfsLocatedFileStatus fileStatus;
  private final int targetRep;
  private final NameNodeConnector nnc;
  private final AzCluster cluster;
  private final AzPolicyProvider azPolicyProvider;
  private final long blockMoveTimeout;
  private final AzMoverMetrics azMoverMetrics;
  private final long createdTime;
  private final long waitTimeToCheck;
  private final List<String> matchAzPolicy;

  public TransferFileTask(
      HdfsLocatedFileStatus fileStatus,
      int targetRep,
      NameNodeConnector nnc,
      AzCluster cluster,
      AzPolicyProvider azPolicyProvider,
      long blockMoveTimeout,
      long waitTimeToCheck,
      List<String> matchAzPolicy) {
    this.fileStatus = fileStatus;
    this.targetRep = targetRep;
    this.nnc = nnc;
    this.azPolicyProvider = azPolicyProvider;
    this.cluster = cluster;
    this.blockMoveTimeout = blockMoveTimeout;
    this.azMoverMetrics = AzMoverMetrics.get(nnc.getDistributedFileSystem().getConf());
    this.waitTimeToCheck = waitTimeToCheck;
    this.createdTime = System.currentTimeMillis();
    this.matchAzPolicy = matchAzPolicy;
  }

  public static Map<String, List<DatanodeInfo>> groupByAz(DatanodeInfo[] datanodeInfos,
      AzCluster cluster) {
    final Map<String, List<DatanodeInfo>> az2DataNodes = new HashMap<>();
    for (DatanodeInfo datanodeInfo : datanodeInfos) {
      String az = cluster.getAz(datanodeInfo);
      az2DataNodes.computeIfAbsent(az, k -> new ArrayList<>()).add(datanodeInfo);
    }
    return az2DataNodes;
  }

  private List<Option> planAndExecute(Consumer<Option> consumer) {
    Path path = fileStatus.getPath();
    List<Option> options = new ArrayList<>();

    if (fileStatus.isErasureCoded()) {
      String mainAz = azPolicyProvider.getAzPolicyInfo(
          AzMoverUtils.getPathWithoutPrefix(path)).getMainAz();
      for (LocatedBlock locatedBlock : fileStatus.getLocatedBlocks().getLocatedBlocks()) {
        LocatedStripedBlock locatedStripedBlock = (LocatedStripedBlock) locatedBlock;
        options.addAll(generateEcOptions(
            cluster,
            locatedStripedBlock,
            cluster.getNotInServiceNodes(),
            fileStatus.getErasureCodingPolicy(),
            mainAz,
            consumer));
      }

      return options;
    }

    List<String> azPolicy = AzMoverUtils.getAzPolicyForTargetRep(path, azPolicyProvider,
        targetRep);

    for (LocatedBlock locatedBlock : fileStatus.getLocatedBlocks().getLocatedBlocks()) {
      DatanodeInfo[] datanodeInfos = filterNotInServiceNodes(locatedBlock.getLocations(),
          cluster.getNotInServiceNodes());
      if (datanodeInfos == null || datanodeInfos.length == 0) {
        LOG.warn("There is no replication: block = blk_{}, path = {}",
            locatedBlock.getBlock().getBlockId(), fileStatus.getPath());
        continue;
      }
      options.addAll(generateOptions(
          cluster,
          datanodeInfos,
          cluster.getNotInServiceNodes(),
          locatedBlock.getBlock(),
          azPolicy,
          consumer,
          matchAzPolicy));
    }
    return options;
  }

  public static LocatedBlock[] getEcBlockLocations(LocatedStripedBlock locatedStripedBlock,
      ErasureCodingPolicy erasureCodingPolicy) {
    return StripedBlockUtil.parseStripedBlockGroup(
        locatedStripedBlock, erasureCodingPolicy.getCellSize(),
        erasureCodingPolicy.getNumDataUnits(), erasureCodingPolicy.getNumParityUnits());
  }

  public static DatanodeInfo[] filterNotInServiceNodes(DatanodeInfo[] datanodeInfos,
      Set<DatanodeInfo> notInServiceNodes) {
    if (datanodeInfos == null || datanodeInfos.length == 0) {
      return datanodeInfos;
    }
    return Arrays.stream(datanodeInfos)
        .filter(datanodeInfo -> !notInServiceNodes.contains(datanodeInfo))
        .toArray(DatanodeInfo[]::new);
  }

  /**
   * 按文件级别，生成 EC block 拷贝任务，任务需要保证顺序执行
   */
  public static List<Option> generateEcOptions(
      final AzCluster cluster,
      final LocatedStripedBlock locatedStripedBlock,
      final Set<DatanodeInfo> notInServiceNodes,
      final ErasureCodingPolicy erasureCodingPolicy,
      final String mainAz,
      @Nullable final Consumer<Option> consumer) {
    List<Option> options = new ArrayList<>();

    List<LocatedBlock> locatedBlocks = new ArrayList<>();
    List<DatanodeInfo> datanodeInfos = new ArrayList<>();
    for (LocatedBlock locatedBlock : getEcBlockLocations(locatedStripedBlock,
        erasureCodingPolicy)) {
      if (locatedBlock == null) {
        continue;
      }
      DatanodeInfo[] locations = filterNotInServiceNodes(locatedBlock.getLocations(),
          notInServiceNodes);
      // 对于 EC block，如果存在于多个 datanode 上，是异常状态，暂时不做迁移
      if (locations.length != 1) {
        LOG.warn("EC block blk_{} exist in multiple datanodes, skip",
            locatedBlock.getBlock().getBlockId());
        return options;
      }
      locatedBlocks.add(locatedBlock);
      datanodeInfos.add(locations[0]);
    }
    for (LocatedBlock locatedBlock : locatedBlocks) {
      ExtendedBlock extendedBlock = locatedBlock.getBlock();
      DatanodeInfo s = locatedBlock.getLocations()[0];
      String az = cluster.getAz(s);
      if (az.equals(mainAz)) {
        continue;
      }

      cluster.chooseDatanodeWithConsumer(mainAz, datanodeInfos, ImmutableList.of(s),
          (source, target) -> {
            if (target == null) {
              throw new IllegalStateException(
                  String.format("Can not get enough node: block = blk_%s, az = %s",
                      extendedBlock.getBlockId(), mainAz));
            }
            Option option = new Option(source, target, source, extendedBlock);
            if (consumer != null) {
              consumer.accept(option);
            }
            options.add(option);

            datanodeInfos.remove(source);
            datanodeInfos.add(option.getTarget());
          });

    }
    return options;
  }

  /**
   * 按文件级别，生成普通 block 的拷贝任务，任务需要保证顺序执行
   */
  @VisibleForTesting
  public static List<Option> generateOptions(
      final AzCluster cluster,
      DatanodeInfo[] datanodeInfos,
      final Set<DatanodeInfo> notInServiceNodes,
      final ExtendedBlock extendedBlock,
      final List<String> azPolicy,
      @Nullable final Consumer<Option> consumer,
      List<String> matchAzPolicy) {

    final List<Option> options = new ArrayList<>();

    datanodeInfos = filterNotInServiceNodes(datanodeInfos, notInServiceNodes);

    if (datanodeInfos.length == 0) {
      return options;
    }

    // current az
    List<String> currentAz = Arrays.stream(datanodeInfos).map(cluster::getAz)
        .collect(Collectors.toList());

    if (matchAzPolicy.size() != 0 && !AzMoverUtils.azPolicyMatch(matchAzPolicy, currentAz)) {
      LOG.info("The distribution of block blk_{} is not match {}, skip it",
          extendedBlock.getBlockId(), matchAzPolicy);
      return ImmutableList.of();
    }

    // az to delete
    LinkedList<String> deletions = new LinkedList<>(currentAz);
    azPolicy.forEach(deletions::remove);

    // az to increment
    LinkedList<String> additions = new LinkedList<>(azPolicy);
    currentAz.forEach(additions::remove);

    // az -> datanodes
    final Map<String, List<DatanodeInfo>> distribution = groupByAz(datanodeInfos, cluster);

    // all nodes
    final List<DatanodeInfo> allNodes = Arrays.stream(datanodeInfos).collect(Collectors.toList());

    if (additions.size() < deletions.size()) {
      LOG.warn(
          "Deletions greater than additions, skip it, blockId = blk_" + extendedBlock.getBlockId());
      return options;
    }

    while (additions.size() > 0) {
      String additionAz = additions.removeFirst();
      // 挑选 source，先看同一 additionAz 有没有，没有就随机选一个
      List<DatanodeInfo> sources = distribution.getOrDefault(additionAz, allNodes);
      // 挑选 target
      cluster.chooseDatanodeWithConsumer(additionAz, allNodes, sources, (source, target) -> {
        if (target == null) {
          throw new IllegalStateException(
              String.format("Can not get enough node: block = blk_%s, az = %s",
                  extendedBlock.getBlockId(), additionAz));
        }
        distribution.computeIfAbsent(additionAz, k -> new ArrayList<>()).add(target);
        allNodes.add(target);
        // 挑选 delete
        DatanodeInfo delete = null;
        if (deletions.size() > 0) {
          String deleteAz = deletions.removeFirst();
          List<DatanodeInfo> nodes = distribution.get(deleteAz);
          if (nodes != null && nodes.size() != 0) {
            // 删除时，尽量找剩余存储最小的 datanode 释放存储
            delete = AzMoverUtils.removeMinRemainingSpaceDatanode(nodes);
            allNodes.remove(delete);
          }
        }
        Option option = new Option(source, target, delete, extendedBlock);
        if (consumer != null) {
          consumer.accept(option);
        }
        options.add(option);
      });
    }
    return options;
  }


  @Override
  public void run() {
    long waitTime = System.currentTimeMillis() - createdTime;
    azMoverMetrics.transferPathsChange(AzMoverMetrics.WAITING, AzMoverMetrics.RUNNING);
    azMoverMetrics.transferFileTaskWaitTimeMs(waitTime);
    Path path = fileStatus.getPath();
    try {
      if (fileStatus.getLen() == 0) {
        LOG.info("The length of file {} is 0, discard it", path);
        azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.DISCARD);
        return;
      }
      if (!fileStatus.isErasureCoded() && fileStatus.getReplication() > targetRep) {
        LOG.info("Target replication is less than file replication for file {}, discard it", path);
        azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.DISCARD);
        return;
      }
      // 迁移之前确认文件是否发生了变化，可能因为排队过久，文件已经删除或者发生了变化
      // 为了调用速度，这里只检查排队超过指定阈值的文件
      if (waitTime > waitTimeToCheck) {
        FileStatus newFileStatus;
        try {
          newFileStatus = nnc.getDistributedFileSystem().getFileStatus(path);
        } catch (FileNotFoundException e) {
          LOG.info("File {} does not exist, discard it", path);
          azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.DISCARD);
          return;
        }
        if (newFileStatus.getModificationTime() != fileStatus.getModificationTime()) {
          LOG.info("Path {} is changed, discard it", path);
          azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.DISCARD);
          return;
        }
      }
      int result = runInternal();
      if (result == 0) {
        azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.DISCARD);
        LOG.info("There is no block to transfer for file {}, discard it", path);
      } else {
        LOG.info("Transfer file {} success", path);
        azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.SUCCESS);
      }
    } catch (IgnoredException ige) {
      LOG.warn(ige.getMessage());
      azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.DISCARD);
    } catch (Exception e) {
      azMoverMetrics.transferPathsChange(AzMoverMetrics.RUNNING, AzMoverMetrics.FAILED);
      LOG.warn("Can not transfer file: {}", path, e);
    }
  }

  public int runInternal() throws IOException {
    try (TimerScope scope = new TimerScope(
        time -> azMoverMetrics.replaceBlockTasksTime(time, AzMoverMetrics.EXECUTE))) {
      Path path = fileStatus.getPath();
      if (fileStatus.getReplication() != targetRep && !fileStatus.isErasureCoded()) {
        LOG.info("Set replication {} for file: {}", targetRep, path);
        nnc.getDistributedFileSystem().setReplication(path, (short) targetRep);
      }
      return planAndExecute(this::execute).size();
    }
  }

  private void execute(Option option) {
    ExtendedBlock extendedBlock = option.getExtendedBlock();
    DatanodeInfo source = option.getSource();
    DatanodeInfo target = option.getTarget();
    DatanodeInfo deletion = option.getDeletion();
    long blockSize = extendedBlock.getNumBytes();
    boolean acrossAz = !cluster.isSameAz(source, target);
    azMoverMetrics.copyingBlockNumber(1, acrossAz);
    try {
      new TransferBlockTask(nnc, fileStatus, source, target, deletion, extendedBlock,
          blockMoveTimeout, cluster).run();
      azMoverMetrics.copiedBytes(blockSize, acrossAz);
      azMoverMetrics.copiedBlockNumber(1, acrossAz);
      if (deletion != null) {
        azMoverMetrics.deletedBytes(blockSize);
        azMoverMetrics.deletedBlockNumber(1);
      }
    } finally {
      azMoverMetrics.copyingBlockNumber(-1, acrossAz);
    }
  }

  public static class Option {

    private final DatanodeInfo source;
    private final DatanodeInfo target;
    private final DatanodeInfo deletion;
    private final ExtendedBlock extendedBlock;

    public Option(DatanodeInfo source, DatanodeInfo target, @Nullable DatanodeInfo deletion,
        ExtendedBlock extendedBlock) {
      this.source = Objects.requireNonNull(source, "Source can not be null");
      this.target = Objects.requireNonNull(target, "Target can not be null");
      this.deletion = deletion;
      if (source.getDatanodeUuid().equals(target.getDatanodeUuid())) {
        throw new IllegalStateException("Source equals target");
      }
      if (deletion != null && target.getDatanodeUuid().equals(deletion.getDatanodeUuid())) {
        throw new IllegalStateException("Target equals deletion");
      }
      this.extendedBlock = extendedBlock;
    }

    public DatanodeInfo getSource() {
      return source;
    }

    public DatanodeInfo getTarget() {
      return target;
    }

    public DatanodeInfo getDeletion() {
      return deletion;
    }

    public ExtendedBlock getExtendedBlock() {
      return extendedBlock;
    }

    @Override
    public String toString() {
      return "Option{" +
          "source=" + source +
          ", target=" + target +
          ", deletion=" + deletion +
          ", extendedBlock=" + extendedBlock +
          '}';
    }
  }
}



