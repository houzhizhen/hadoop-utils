package org.apache.hadoop.hdfs.server.azmover;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.source.JvmMetrics;

@InterfaceAudience.Private
@Metrics(name = "AzMoverMetrics", about = "Metrics for AzMover Process Interceptor", context = "dfs")
public class AzMoverMetrics {

  private static AzMoverMetrics INSTANCE;

  public synchronized static AzMoverMetrics get(Configuration conf) {
    if (INSTANCE == null) {
      String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
      MetricsSystem metricsSystem = DefaultMetricsSystem.initialize("AzMover");
      JvmMetrics jm = JvmMetrics.create("azmover", sessionId, metricsSystem);
      INSTANCE = metricsSystem.register("AzMover", null, new AzMoverMetrics(jm));
      INSTANCE.indexMetrics();
    }
    return INSTANCE;
  }

  private final JvmMetrics jvmMetrics;

  public AzMoverMetrics(JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  //---------------------------------------------------------------------------------------

  @Metric
  MutableGaugeLong copyingAcrossAzBlocksNumber;
  @Metric
  MutableGaugeLong copyingSameAzBlocksNumber;
  @Metric
  MutableGaugeLong copiedAcrossAzBlocksNumber;
  @Metric
  MutableGaugeLong copiedSameAzBlocksNumber;
  @Metric
  MutableGaugeLong deleteBlocksNumber;

  public void copyingBlockNumber(int num, boolean acrossAz) {
    if (acrossAz) {
      copyingAcrossAzBlocksNumber.incr(num);
    } else {
      copyingSameAzBlocksNumber.incr(num);
    }
  }

  public void copiedBlockNumber(int num, boolean acrossAz) {
    if (acrossAz) {
      copiedAcrossAzBlocksNumber.incr(num);
    } else {
      copiedSameAzBlocksNumber.incr(num);
    }
  }

  public void deletedBlockNumber(int num) {
    deleteBlocksNumber.incr(num);
  }

  //---------------------------------------------------------------------------------------

  @Metric
  MutableGaugeLong copiedAcrossAzBytes;
  @Metric
  MutableGaugeLong copiedSameAzBytes;
  @Metric
  MutableGaugeLong deletedBytes;

  public void copiedBytes(long num, boolean acrossAz) {
    if (acrossAz) {
      copiedAcrossAzBytes.incr(num);
    } else {
      copiedSameAzBytes.incr(num);
    }
  }

  public void deletedBytes(long num) {
    deletedBytes.incr(num);
  }

  //---------------------------------------------------------------------------------------

  public static final String VISITED = "VISITED";
  public static final String SUBMITTED = "SUBMITTED";
  public static final String WAITING = "WAITING";
  public static final String RUNNING = "RUNNING";
  public static final String SUCCESS = "SUCCESS";
  public static final String FAILED = "FAILED";
  public static final String TIMEOUT = "TIMEOUT";
  public static final String DISCARD = "DISCARD";

  @Metric
  MutableGaugeLong visitedPathsNumber;
  @Metric
  MutableGaugeLong submittedPathsNumber;
  @Metric
  MutableGaugeLong waitingPathsNumber;
  @Metric
  MutableGaugeLong runningPathsNumber;
  @Metric
  MutableGaugeLong successPathsNumber;
  @Metric
  MutableGaugeLong failedPathsNumber;
  @Metric
  MutableGaugeLong timeoutPathsNumber;
  @Metric
  MutableGaugeLong discardPathsNumber;


  private Map<String, MutableGaugeLong> pathMetricsIndex;

  public void transferPathsChange(String srcState, String dstState) {
    if (srcState != null) {
      pathMetricsIndex.get(srcState).decr();
    }
    if (dstState != null) {
      pathMetricsIndex.get(dstState).incr();
    }
  }

  //---------------------------------------------------------------------------------------

  public static final String PLAN = "plan";
  public static final String EXECUTE = "execute";

  @Metric
  MutableGaugeLong planTasksTimeMs;
  @Metric
  MutableGaugeLong executeTasksTimeMs;

  private Map<String, MutableGaugeLong> replaceBlockTasksMetricsIndex;

  public void replaceBlockTasksTime(long time, String type) {
    replaceBlockTasksMetricsIndex.get(type).incr(time);
  }

  //---------------------------------------------------------------------------------------

  public void indexMetrics() {
    this.pathMetricsIndex = ImmutableMap.<String, MutableGaugeLong>builder()
        .put(SUBMITTED, submittedPathsNumber)
        .put(WAITING, waitingPathsNumber)
        .put(RUNNING, runningPathsNumber)
        .put(SUCCESS, successPathsNumber)
        .put(FAILED, failedPathsNumber)
        .put(TIMEOUT, timeoutPathsNumber)
        .put(DISCARD, discardPathsNumber)
        .put(VISITED, visitedPathsNumber)
        .build();
    this.replaceBlockTasksMetricsIndex = ImmutableMap.<String, MutableGaugeLong>builder()
        .put(PLAN, planTasksTimeMs)
        .put(EXECUTE, executeTasksTimeMs)
        .build();
  }

  //---------------------------------------------------------------------------------------

  @Metric
  MutableGaugeLong maxWritingThreadPerDatanode;

  public void setMaxWritingThreadPerDatanode(long value) {
    maxWritingThreadPerDatanode.set(value);
  }

  @Metric
  MutableGaugeLong maxReadingThreadPerDatanode;

  public void setMaxReadingThreadPerDatanode(long value) {
    maxReadingThreadPerDatanode.set(value);
  }

  @Metric
  MutableGaugeLong writingDatanodeCount;

  public void setWritingDatanodeCount(long num) {
    writingDatanodeCount.set(num);
  }

  @Metric
  MutableGaugeLong readingDatanodeCount;

  public void setReadingDatanodeCount(long num) {
    readingDatanodeCount.set(num);
  }

  //---------------------------------------------------------------------------------------
  @Metric
  MutableGaugeLong parallelism;

  public void setParallelism(long num) {
    parallelism.set(num);
  }

  //---------------------------------------------------------------------------------------
  @Metric
  MutableGaugeLong chooseDatanodeTimeNs;
  @Metric
  MutableGaugeLong chooseDatanodeCount;

  public void chooseDatanodeTimeNs(long time) {
    chooseDatanodeTimeNs.incr(time);
    chooseDatanodeCount.incr();
  }

  //---------------------------------------------------------------------------------------
  @Metric
  MutableGaugeLong transferFileTaskWaitTimeMs;

  @Metric
  MutableGaugeLong transferFileTaskWaitCount;

  public void transferFileTaskWaitTimeMs(long time) {
    transferFileTaskWaitTimeMs.incr(time);
    transferFileTaskWaitCount.incr();
  }


}

