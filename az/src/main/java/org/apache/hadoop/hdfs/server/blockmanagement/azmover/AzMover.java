package org.apache.hadoop.hdfs.server.azmover;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_IMPL_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.MOVER_QUEUE_SIZE_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.MOVE_BLOCK_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.MOVE_BLOCK_TIMEOUT_KEY_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.WAIT_TIME_TO_CHECK_MS_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.WAIT_TIME_TO_CHECK_MS_DEFAULT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.azmover.FileStatusVisitor.FileStatusFilter;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzPolicyProvider;
import org.apache.hadoop.hdfs.server.blockmanagement.az.LocalFileAzPolicyProvider;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzMover {

  static final Logger LOG = LoggerFactory.getLogger(AzMover.class);

  private final Configuration conf;
  private final NameNodeConnector nameNodeConnector;
  private final AzCluster cluster;
  private final AzPolicyProvider azPolicyProvider;
  private final long blockMoveTimeout;
  private final long waitTimeToCheck;
  private final ThreadPoolExecutor moverExecutor;
  private final FileStatusVisitor visitor;
  private final ArrayBlockingQueue<Runnable> moverQueue;
  private final AzMoverParams params;
  private final AzMoverMetrics azMoverMetrics;

  public AzMover(AzMoverParams params, Configuration conf) {
    this.params = params;
    this.conf = conf;
    this.azMoverMetrics = AzMoverMetrics.get(conf);
    azMoverMetrics.setParallelism(params.getParallelism());
    this.azPolicyProvider = createAzPolicyProvider();
    this.nameNodeConnector = getNameNodeConnector();
    this.cluster = new AzCluster(conf, params.getNodeSelector(), () -> {
      try {
        return nameNodeConnector.getLiveDatanodeStorageReport();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    });
    this.blockMoveTimeout = conf.getLong(MOVE_BLOCK_TIMEOUT_KEY,
        MOVE_BLOCK_TIMEOUT_KEY_DEFAULT);
    this.waitTimeToCheck = conf.getLong(WAIT_TIME_TO_CHECK_MS_KEY, WAIT_TIME_TO_CHECK_MS_DEFAULT);
    int moverThreadNum = params.getParallelism();
    int queueSize = conf.getInt(MOVER_QUEUE_SIZE_KEY, QUEUE_SIZE_DEFAULT);
    this.moverQueue = new ArrayBlockingQueue<>(queueSize);
    this.moverExecutor = new ThreadPoolExecutor(moverThreadNum, moverThreadNum, 0L,
        TimeUnit.MILLISECONDS, moverQueue, (r, executor) -> {
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    this.visitor = new FileStatusVisitor(params.getListeningThread());
  }

  private AzPolicyProvider createAzPolicyProvider() {
    Class<? extends AzPolicyProvider> azPolicyProviderClass = conf.getClass(
        AZ_POLICY_PROVIDER_IMPL_KEY, LocalFileAzPolicyProvider.class, AzPolicyProvider.class);
    AzPolicyProvider azPolicyProvider = ReflectionUtils.newInstance(azPolicyProviderClass, conf);
    long period = conf.getLong(AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_KEY, 0L);
    if (period == 0) {
      // disable auto update az policy unless period is configured.
      conf.setLong(AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_KEY, 0L);
    }
    azPolicyProvider.initialize(conf, null);
    return azPolicyProvider;
  }

  private List<File> getInputFiles(String input) throws FileNotFoundException {
    File parent = new File(input);
    if (!parent.exists()) {
      throw new FileNotFoundException("Local input file does not exist " + input);
    }
    List<File> files = new ArrayList<>();
    if (parent.isFile()) {
      files.add(parent);
    } else {
      Optional.ofNullable(parent.listFiles(File::isFile)).ifPresent(children -> files.addAll(
          Arrays.asList(children)));
    }
    return files;
  }

  private void submitByFiles(List<File> files) throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(Math.min(files.size(), 5));
    for (File file : files) {
      threadPool.execute(() -> {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(Files.newInputStream(file.toPath())))) {
          String line;
          while ((line = reader.readLine()) != null) {
            if (skipPath(line)) {
              continue;
            }
            Path path = new Path(line);
            transferInternal(path, nameNodeConnector, cluster, getFilters());
          }
        } catch (Exception e) {
          LOG.warn("Read file {} error", file.toString(), e);
        }
      });
    }
    threadPool.shutdown();
    while (!threadPool.isTerminated()) {
      Thread.sleep(1000L);
    }
  }


  public int run() {
    try {
      doRun();
    } catch (Exception e) {
      return 1;
    }
    return 0;
  }


  public void doRun() throws IOException, InterruptedException {
    String input = params.getInput();
    if (input != null) {
      LOG.info("Use input files, ignore path args.");
      List<File> files = getInputFiles(input);
      if (files.size() == 0) {
        return;
      }
      submitByFiles(files);
    } else {
      Path path = new Path(params.getPath());
      if (skipPath(path.toString())) {
        return;
      }
      transferInternal(path, nameNodeConnector, cluster, getFilters());
    }
    // waiting for visitor finished
    while (visitor.isRunning() || moverQueue.size() != 0) {
      Thread.sleep(1000L);
    }
    LOG.info("Mover queue is empty now, shutdown mover executor.");
    moverExecutor.shutdown();
    while (!moverExecutor.isTerminated()) {
      Thread.sleep(1000L);
    }
    LOG.info("Shutdown file visitor.");
    visitor.shutdown();
    LOG.info("Shutdown az cluster.");
    cluster.shutdown();
    LOG.info("AzMover finished.");
  }

  private List<FileStatusFilter> getFilters() {
    // add extra filters
    List<FileStatusFilter> filters = new ArrayList<>(params.getFilters());

    // path regex filter
    List<FileStatusFilter> pathRegexFilters = params.getExcludedPaths().stream()
        .map(regex -> (FileStatusFilter) fileStatus -> !AzMoverUtils.getPathWithoutPrefix(
            fileStatus.getPath()).matches(regex))
        .collect(Collectors.toList());
    filters.addAll(pathRegexFilters);

    // length filter
    filters.add(fileStatus -> !(fileStatus.isFile() && fileStatus.getLen() == 0));

    return filters;
  }

  private void transferInternal(Path path, NameNodeConnector nnc,
      AzCluster cluster, List<FileStatusFilter> filters) {
    visitor.visit(nnc.getDistributedFileSystem(), path, filters, fileStatus -> {
      TransferFileTask task = new TransferFileTask(fileStatus, params.getTargetRep(), nnc,
          cluster, azPolicyProvider, blockMoveTimeout, waitTimeToCheck, params.getMatchAzPolicy());
      moverExecutor.execute(task);
      azMoverMetrics.transferPathsChange(null, AzMoverMetrics.SUBMITTED);
      azMoverMetrics.transferPathsChange(null, AzMoverMetrics.WAITING);
    });
  }

  /**
   * excludedPaths，黑名单，只要有一个匹配，就跳过
   */
  private boolean skipPath(String path) {
    Set<String> excludedPaths = params.getExcludedPaths();
    if (excludedPaths != null && !excludedPaths.isEmpty()) {
      if (excludedPaths.stream().anyMatch(path::matches)) {
        return true;
      }
    }
    return false;
  }

  private NameNodeConnector getNameNodeConnector() {
    return DFSUtil.getInternalNsRpcUris(conf).stream()
        .filter(uri -> uri.getHost().equals(params.getNameService()))
        .findFirst()
        .map(uri ->
        {
          try {
            return NameNodeConnector.newNameNodeConnectors(Collections.singletonList(uri),
                    AzMover.class.getSimpleName(), AzConstant.AZMOVER_ID_PATH, conf, 5)
                .get(0);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }).orElseThrow(() -> new IllegalStateException(
            "Can not get namenode connector for nameService: " + params.getNameService()));
  }
}

