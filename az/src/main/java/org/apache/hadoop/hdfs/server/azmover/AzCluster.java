package org.apache.hadoop.hdfs.server.azmover;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.CHOOSE_SOURCE_WITH_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.CHOOSE_SOURCE_WITH_RETRY_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DATANODE_MAX_READ_THREAD_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DATANODE_MAX_READ_THREAD_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DATANODE_MIN_REMAINING_BYTES;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DATANODE_MIN_REMAINING_BYTES_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.UPDATE_DATANODE_REPORTS_PERIOD;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.UPDATE_DATANODE_REPORTS_PERIOD_DEFAULT;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 经过测试，这个锁非常轻量，放心使用
 */
public class AzCluster {

  private static final Logger LOG = LoggerFactory.getLogger(AzCluster.class);

  private final Configuration conf;
  private final long minRemaining;
  private final Map<String, Integer> nodeSelector;
  private final AzMoverMetrics azMoverMetrics;
  private final long maxReadThread;
  private final boolean chooseSourceWithRetry;
  private Set<DatanodeInfo> notInServiceNodes = ImmutableSet.of();
  private NetworkTopology networkTopology;
  // 堆排序
  // target
  private Map<DatanodeInfo, AtomicInteger> targetThreadCounter = ImmutableMap.of();
  private Map<String, TreeSet<DataNode>> targets = ImmutableMap.of();
  // source
  private final Map<DatanodeInfo, AtomicInteger> sourceThreadCounter = new HashMap<>();
  private Timer timer;
  private final Map<DatanodeInfo, SuccessRateCalculator> successRate = new ConcurrentHashMap<>();

  @VisibleForTesting
  public AzCluster(Configuration conf, NetworkTopology networkTopology) {
    this(conf, networkTopology.getLeaves("").stream().map(node -> (DatanodeInfo) node)
        .collect(Collectors.toList()));
  }

  @VisibleForTesting
  public AzCluster(Configuration conf, List<DatanodeInfo> datanodeInfos) {
    this(conf,
        ImmutableMap.of(),
        () -> datanodeInfos.stream()
            .map(datanodeInfo -> new DatanodeStorageReport(datanodeInfo, StorageReport.EMPTY_ARRAY))
            .toArray(DatanodeStorageReport[]::new));
  }

  public AzCluster(Configuration conf,
      Map<String, Integer> nodeSelector,
      Supplier<DatanodeStorageReport[]> reportsSupplier) {
    this.conf = conf;
    this.azMoverMetrics = AzMoverMetrics.get(conf);
    this.nodeSelector = nodeSelector;
    this.minRemaining = conf.getLong(DATANODE_MIN_REMAINING_BYTES,
        DATANODE_MIN_REMAINING_BYTES_DEFAULT);
    this.maxReadThread = conf.getLong(DATANODE_MAX_READ_THREAD_KEY,
        DATANODE_MAX_READ_THREAD_DEFAULT);
    this.chooseSourceWithRetry = conf.getBoolean(CHOOSE_SOURCE_WITH_RETRY_KEY,
        CHOOSE_SOURCE_WITH_RETRY_DEFAULT);
    updateNetworkTopology(reportsSupplier);
    long updateReportsPeriod = conf.getLong(UPDATE_DATANODE_REPORTS_PERIOD,
        UPDATE_DATANODE_REPORTS_PERIOD_DEFAULT);
    this.timer = new Timer();
    if (updateReportsPeriod > 0) {
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            LOG.info("Start update networkTopology");
            updateNetworkTopology(reportsSupplier);
            LOG.info("Update networkTopology success");
          } catch (Exception e) {
            LOG.warn("Update networkTopology failed", e);
          }
        }
      }, updateReportsPeriod, updateReportsPeriod);
    }
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          updateRunningThread();
        } catch (Exception e) {
          LOG.warn("Can not update max running thread", e);
        }
      }
    }, 1000L, 1000L);
    // 1min 更新一次成功率的统计
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          successRate.values().forEach(SuccessRateCalculator::clean);
        } catch (Exception e) {
          LOG.warn("Can not clear ");
        }
      }
    }, 600 * 1000L, 600 * 1000L);
  }

  private void updateNetworkTopology(
      Supplier<DatanodeStorageReport[]> reportsSupplier) {
    DatanodeStorageReport[] reports = reportsSupplier.get();
    NetworkTopology networkTopology = NetworkTopology.getInstance(conf);
    Set<DatanodeInfo> notInServiceNodes = new HashSet<>();
    List<DatanodeStorageReport> shuffledReports = new ArrayList<>(Arrays.asList(reports));
    Collections.shuffle(shuffledReports);
    for (DatanodeStorageReport r : shuffledReports) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      successRate.putIfAbsent(datanode, new SuccessRateCalculator());
      if (!datanode.isInService()) {
        LOG.info("Ignore datanode: {}, because it not in service", datanode.getHostName());
        notInServiceNodes.add(datanode);
        continue;
      }
      if (datanode.getRemaining() <= minRemaining) {
        LOG.info("Ignore datanode: {}, because it doesn't have enough space",
            datanode.getHostName());
        continue;
      }
      SuccessRateCalculator calculator = successRate.get(datanode);
      // 这个数据写死，没什么好改的
      if (calculator.getSuccessRate() < 0.9 && calculator.getTotal() >= 100) {
        LOG.info("Ignore datanode: {}, because success rate {} is less than 0.9",
            datanode.getHostName(), calculator.getSuccessRate());
        continue;
      }
      networkTopology.add(datanode);
    }

    // 根据 node selector 挑选指定数量的节点
    // 挑选逻辑如下，挑选节点空闲存储的 2/3 的机器，再到这 2/3 的机器随机挑选指定台数的机器
    nodeSelector.forEach((az, number) -> {
      List<Node> nodesInAz = networkTopology.getLeaves(az).stream()
          .map(node -> (DatanodeInfo) node)
          .sorted((a, b) -> -this.compareDataNodeRemaining(a, b))
          .collect(Collectors.toList());
      // 先移除，再添加回来
      nodesInAz.forEach(networkTopology::remove);
      // 选取 2/3
      List<Node> nodes = nodesInAz.subList(0,
          Math.min(Math.max(nodesInAz.size() * 2 / 3, number), nodesInAz.size()));
      // 选取 node selector 的指定个数
      Collections.shuffle(nodes);
      for (int i = 0; i < nodes.size() && i < number; i++) {
        networkTopology.add(nodes.get(i));
      }
    });

    List<DatanodeInfo> dataNodes = getFqdnSortedDataNodes(networkTopology);

    // update target counter
    Map<DatanodeInfo, AtomicInteger> targetThreadCounter = new HashMap<>(this.targetThreadCounter);
    dataNodes.forEach(datanodeInfo -> {
      if (!targetThreadCounter.containsKey(datanodeInfo)) {
        targetThreadCounter.put(datanodeInfo, new AtomicInteger(0));
      }
    });

    Map<String, TreeSet<DataNode>> heapSort = getHeapSort(networkTopology, targetThreadCounter);

    synchronized (this) {
      this.targetThreadCounter = ImmutableMap.copyOf(targetThreadCounter);
      this.notInServiceNodes = ImmutableSet.copyOf(notInServiceNodes);
      this.networkTopology = networkTopology;
      this.targets = ImmutableMap.copyOf(heapSort);
    }

    logSelectedDataNodes(dataNodes);
    logSources();
    logTargets();
  }

  private void logSelectedDataNodes(List<DatanodeInfo> dataNodes) {
    long gb = 1024 * 1024 * 1024;
    String msg = dataNodes.stream()
        .map(
            datanodeInfo -> String.format(
                "host = %s,\tip = %s,\track = %s,\taz = %s, \t remaining = %sGB",
                datanodeInfo.getHostName(),
                datanodeInfo.getIpAddr(),
                datanodeInfo.getNetworkLocation(),
                getAz(datanodeInfo),
                datanodeInfo.getRemaining() / gb))
        .collect(Collectors.joining("\n"));

    LOG.info("Current datanodes for targets are:\n{}", msg);
  }

  public synchronized Map<DatanodeInfo, Integer> snapshotThreadCounter(
      Map<DatanodeInfo, AtomicInteger> threadCounter) {
    return threadCounter.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get()));
  }

  private void logSources() {
    AtomicInteger sum = new AtomicInteger(0);
    String msg = snapshotThreadCounter(this.sourceThreadCounter).entrySet().stream()
        .filter(e -> e.getValue() > 0)
        .peek(e -> sum.addAndGet(e.getValue()))
        .sorted((e1, e2) -> AzMoverUtils.getFqdnComparator()
            .compare(e1.getKey().getHostName(), e2.getKey().getHostName()))
        .map(e -> String.format("host = %s,\trunning = %s", e.getKey().getHostName(), e.getValue()))
        .collect(Collectors.joining("\n"));
    LOG.info("Current running threads is {}, threads for each source datanode are:\n{}", sum.get(),
        msg);
  }

  private void logTargets() {
    AtomicInteger sum = new AtomicInteger(0);
    String msg = snapshotThreadCounter(this.targetThreadCounter).entrySet().stream()
        .filter(e -> e.getValue() > 0)
        .peek(e -> sum.addAndGet(e.getValue()))
        .sorted((e1, e2) -> AzMoverUtils.getFqdnComparator()
            .compare(e1.getKey().getHostName(), e2.getKey().getHostName()))
        .map(e -> String.format("host = %s,\trunning = %s", e.getKey().getHostName(), e.getValue()))
        .collect(Collectors.joining("\n"));
    LOG.info("Current running threads is {}, threads for each target datanode are:\n{}", sum.get(),
        msg);
  }

  private synchronized Map<String, TreeSet<DataNode>> getHeapSort(NetworkTopology networkTopology,
      Map<DatanodeInfo, AtomicInteger> threadCounter) {
    Map<String, TreeSet<DataNode>> heapSort = new HashMap<>();
    for (Node node : networkTopology.getLeaves("")) {
      DatanodeInfo datanodeInfo = (DatanodeInfo) node;
      String az = getAz(datanodeInfo);
      TreeSet<DataNode> heap = heapSort.computeIfAbsent(az, k -> new TreeSet<>());
      AtomicInteger runningThread = threadCounter.get(datanodeInfo);
      heap.add(new DataNode(datanodeInfo, runningThread));
    }
    return heapSort;
  }

  // get all datanodes sorted by FQDN prefix
  private List<DatanodeInfo> getFqdnSortedDataNodes(NetworkTopology cluster) {
    return cluster.getLeaves("").stream()
        .filter(node -> node instanceof DatanodeInfo)
        .map(node -> (DatanodeInfo) node)
        .sorted((data1, data2) -> AzMoverUtils.getFqdnComparator()
            .compare(data1.getHostName(), data2.getHostName())).collect(Collectors.toList());
  }

  public Set<DatanodeInfo> getNotInServiceNodes() {
    return notInServiceNodes;
  }

  public String getAz(DatanodeInfo datanodeInfo) {
    return AzUtils.getAzFromRackLocation(datanodeInfo.getNetworkLocation());
  }

  public boolean isSameAz(DatanodeInfo a, DatanodeInfo b) {
    return Objects.equals(getAz(a), getAz(b));
  }

  public synchronized DatanodeInfo chooseSource(Collection<DatanodeInfo> sources) {
    DatanodeInfo source = sources.stream()
        .min(Comparator.comparingInt(
            datanodeInfo -> sourceThreadCounter.getOrDefault(datanodeInfo, new AtomicInteger(0))
                .get()))
        .orElse(null);
    if (source == null) {
      return null;
    }
    AtomicInteger readThread = sourceThreadCounter.computeIfAbsent(source,
        k -> new AtomicInteger(0));
    if (readThread.get() >= maxReadThread) {
      throw new SourceReadLimitException(
          String.format("Source datanode: %s read thread reach limit %s",
              source.getHostName(), maxReadThread));
    }
    readThread.incrementAndGet();
    return source;
  }

  private DatanodeInfo chooseSourceWithRetry(Collection<DatanodeInfo> sources) {
    while (true) {
      try {
        try (TimerScope timerScope = new TimerScope(azMoverMetrics::chooseDatanodeTimeNs, true)) {
          return chooseSource(sources);
        }
      } catch (SourceReadLimitException e) {
        if (!chooseSourceWithRetry) {
          throw e;
        }
        LOG.warn("Retry choose source due to: {}", e.getMessage());
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ex) {
          throw new RuntimeException("Thread interrupted", e);
        }
      }
    }
  }

  private synchronized void releaseSource(DatanodeInfo datanodeInfo) {
    sourceThreadCounter.get(datanodeInfo).decrementAndGet();
  }

  private synchronized DatanodeInfo chooseTarget(String az, Set<DatanodeInfo> excludedNodes) {
    TreeSet<DataNode> heap = targets.get(az);
    DatanodeInfo datanodeInfo = null;
    if (heap != null) {
      Iterator<DataNode> iterator = heap.iterator();
      while (iterator.hasNext()) {
        DataNode dataNode = iterator.next();
        if (excludedNodes.contains(dataNode.datanodeInfo)) {
          continue;
        }
        // resort
        iterator.remove();
        dataNode.runningThread.incrementAndGet();
        heap.add(dataNode);
        datanodeInfo = dataNode.datanodeInfo;
        break;
      }
    }

    return datanodeInfo;
  }

  private synchronized void updateRunningThread() {
    AtomicInteger maxWritingThreadPerDatanode = new AtomicInteger(0);
    AtomicInteger writingDatanodeCount = new AtomicInteger(0);
    targets.values().stream().flatMap(Collection::stream)
        .map(dataNode -> dataNode.runningThread.get())
        .filter(i -> i != 0)
        .forEach(i -> {
          maxWritingThreadPerDatanode.set(Math.max(maxWritingThreadPerDatanode.get(), i));
          writingDatanodeCount.incrementAndGet();
        });
    azMoverMetrics.setMaxWritingThreadPerDatanode(maxWritingThreadPerDatanode.get());
    azMoverMetrics.setWritingDatanodeCount(writingDatanodeCount.get());

    AtomicInteger maxReadingThreadPerDatanode = new AtomicInteger(0);
    AtomicInteger readingDatanodeCount = new AtomicInteger(0);
    sourceThreadCounter.values().stream()
        .map(AtomicInteger::get)
        .filter(i -> i != 0)
        .forEach(i -> {
          maxReadingThreadPerDatanode.set(Math.max(maxReadingThreadPerDatanode.get(), i));
          readingDatanodeCount.incrementAndGet();
        });
    azMoverMetrics.setMaxReadingThreadPerDatanode(maxReadingThreadPerDatanode.get());
    azMoverMetrics.setReadingDatanodeCount(readingDatanodeCount.get());
  }

  private synchronized void releaseTarget(DatanodeInfo datanodeInfo) {
    TreeSet<DataNode> heap = targets.get(getAz(datanodeInfo));
    AtomicInteger runningThread = targetThreadCounter.get(datanodeInfo);
    DataNode dataNode = new DataNode(datanodeInfo, runningThread);
    boolean remove = heap.remove(dataNode);
    runningThread.decrementAndGet();
    if (remove) {
      heap.add(dataNode);
    }
  }

  public void chooseDatanodeWithConsumer(String az, Collection<DatanodeInfo> excludedNodes,
      Collection<DatanodeInfo> sourceCandidates, BiConsumer<DatanodeInfo, DatanodeInfo> consumer) {
    Set<DatanodeInfo> excludedNodesSet = new HashSet<>(excludedNodes);
    DatanodeInfo source = chooseSourceWithRetry(sourceCandidates);
    DatanodeInfo target;
    try (TimerScope timerScope = new TimerScope(azMoverMetrics::chooseDatanodeTimeNs, true)) {
      target = chooseTarget(az, excludedNodesSet);
      if (target != null) {
        successRate.get(target).increaseSuccess();
      }
    }
    try {
      consumer.accept(source, target);
    } catch (Exception e) {
      // 这里只对 target 计数，source 机器挂了是可以容忍的，等待 namenode 将其移除即可
      // target 机器挂了会导致短时间内任务打到同一机器上，因为当前写入是按照写入线程平均分配
      if (target != null) {
        successRate.get(target).increaseFailed();
      }
      throw e;
    } finally {
      if (source != null) {
        releaseSource(source);
      }
      if (target != null) {
        releaseTarget(target);
      }
    }
  }


  /**
   * 作为 comparator 是剩余空间从小到大.
   */
  private int compareDataNodeRemaining(final DatanodeInfo a, final DatanodeInfo b) {
    if (a.equals(b) || a.getRemaining() == b.getRemaining()) {
      return 0;
    }
    return a.getRemaining() - b.getRemaining() > 0 ? 1 : -1;
  }

  public void shutdown() {
    Optional.ofNullable(timer).ifPresent(Timer::cancel);
  }

  public static class DataNode implements Comparable<DataNode> {

    private final DatanodeInfo datanodeInfo;
    private final AtomicInteger runningThread;

    public DataNode(@Nonnull DatanodeInfo datanodeInfo, @Nonnull AtomicInteger runningThread) {
      this.datanodeInfo = datanodeInfo;
      this.runningThread = runningThread;
    }

    @Override
    public int compareTo(@Nonnull DataNode that) {
      if (this.equals(that)) {
        return 0;
      }
      return ComparisonChain.start()
          .compare(this.runningThread.get(), that.runningThread.get())
          .compare(this.datanodeInfo, that.datanodeInfo)
          .result();
    }

    @Override
    public String toString() {
      return "DataNode{" +
          "datanodeInfo=" + datanodeInfo.getHostName() +
          ", runningThread=" + runningThread.get() +
          '}';
    }
  }

  public static class SuccessRateCalculator {

    private final AtomicLong failed;
    private final AtomicLong success;

    public SuccessRateCalculator() {
      this.failed = new AtomicLong(0);
      this.success = new AtomicLong(0);
    }

    public synchronized void increaseSuccess() {
      success.incrementAndGet();
    }

    public synchronized void increaseFailed() {
      failed.incrementAndGet();
    }

    public synchronized long getTotal() {
      return success.get() + failed.get();
    }

    public synchronized double getSuccessRate() {
      long total = getTotal();
      if (total == 0) {
        return 1;
      }
      return 1.0 * success.get() / total;
    }

    public synchronized void clean() {
      success.set(0);
      failed.set(0);
    }
  }

}

