package org.apache.hadoop.hdfs.server.blockmanagement.az;

import org.apache.hadoop.fs.Path;

public class AzConstant {

  public static final String AZ_POLICY_PROVIDER_IMPL_KEY = "az.policy.provider.impl";
  public static final String AZ_POLICY_PROVIDER_LOCAL_FILE_PATH_KEY = "az.policy.provider.local-file.path";
  // az policy 更新周期，配置小于等于 0 代表不更新
  public static final String AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_KEY = "az.policy.provider.local-file.update-period-ms";
  public static final long AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_DEFAULT = 10_000L;
  public static final String UPDATE_AZ_POLICY_TASK_NAME = "UpdateAzPolicyTask";
  // az policy 历史版本存放路径，不配置则代表不存储历史版本
  public static final String AZ_POLICY_PROVIDER_LOCAL_FILE_HISTORICAL_VERSIONS_PATH_KEY = "az.policy.provider.local-file.historical-versions.path";

  public static final String DNS_SWITCH_MAPPING_LOCAL_FILE_PATH_KEY = "dns.switch.mapping.local-file.path";
  // 网络拓扑更新周期，配置小于等于 0 代表不更新
  public static final String DNS_SWITCH_MAPPING_LOCAL_FILE_UPDATE_PERIOD_MS_KEY = "dns.switch.mapping.local-file.update-period-ms";
  public static final long DNS_SWITCH_MAPPING_LOCAL_FILE_UPDATE_PERIOD_MS_DEFAULT = 10_000L;
  public static final String UPDATE_DNS_MAPPING_TASK_NAME = "UpdateDnsMappingTask";

  public static final String MOVER_QUEUE_SIZE_KEY = "azmover.mover.queue-size";
  public static final int QUEUE_SIZE_DEFAULT = 1_000_000;

  public static final String UPDATE_DATANODE_REPORTS_PERIOD = "azmover.update-datanode-reports.period";
  public static final long UPDATE_DATANODE_REPORTS_PERIOD_DEFAULT = 10 * 60 * 1000L;

  public static final String MOVE_BLOCK_TIMEOUT_KEY = "azmover.move-block-timeout";
  public static final long MOVE_BLOCK_TIMEOUT_KEY_DEFAULT = 30 * 60 * 1000L;

  public static final String DATANODE_MIN_REMAINING_BYTES = "azmover.datanode.min-remaining-bytes";
  public static final long DATANODE_MIN_REMAINING_BYTES_DEFAULT = 0;

  public static final String WAIT_TIME_TO_CHECK_MS_KEY = "azmover.wait-time-to-check-ms";
  public static final long WAIT_TIME_TO_CHECK_MS_DEFAULT = 60 * 1000L;

  public static final String DATANODE_MAX_READ_THREAD_KEY = "azmover.datanode.max-read-thread";
  public static final long DATANODE_MAX_READ_THREAD_DEFAULT = 20L;

  public static final String CHOOSE_SOURCE_WITH_RETRY_KEY = "azmover.choose-source-with-retry";
  public static final boolean CHOOSE_SOURCE_WITH_RETRY_DEFAULT = true;

  public static final Path AZMOVER_ID_PATH = new Path("/system/azmover.id");

  public static final String[] EMPTY_STRING_ARRAY = new String[0];


}

