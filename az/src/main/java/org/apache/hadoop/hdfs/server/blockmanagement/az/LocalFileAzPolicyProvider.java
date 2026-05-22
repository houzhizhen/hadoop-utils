package org.apache.hadoop.hdfs.server.blockmanagement.az;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_LOCAL_FILE_HISTORICAL_VERSIONS_PATH_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_LOCAL_FILE_PATH_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.UPDATE_AZ_POLICY_TASK_NAME;

/**
 * 从文件读取 az policy
 */
public class LocalFileAzPolicyProvider extends AzPolicyProvider {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileAzPolicyProvider.class);

  private static final String CURRENT_AZ_POLICY = "AZ_POLICY.current.json";
  private static final String PREVIOUS_AZ_POLICY_FILE_NAME_FORMAT = "AZ_POLICY.%s.json";

  private final ObjectMapper mapper = new ObjectMapper();
  private final TypeReference<TreeMap<String, AzPolicyInfo>> typeReference = new TypeReference<TreeMap<String, AzPolicyInfo>>() {
  };

  private String filePath;
  private String md5Hex;
  private String historicalVersionsPath;

  @Override
  public void initialize(Configuration conf, BlockType blockType) {
    super.initialize(conf, blockType);
    this.filePath = Preconditions.checkNotNull(conf.get(AZ_POLICY_PROVIDER_LOCAL_FILE_PATH_KEY),
        "%s can not be null", AZ_POLICY_PROVIDER_LOCAL_FILE_PATH_KEY);
    this.historicalVersionsPath = conf.get(
        AZ_POLICY_PROVIDER_LOCAL_FILE_HISTORICAL_VERSIONS_PATH_KEY);
    updateAzPolicies();
    long period = conf.getLong(AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_KEY,
        AZ_POLICY_PROVIDER_LOCAL_FILE_UPDATE_PERIOD_MS_DEFAULT);
    if (period > 0) {
      String name = blockType == null ? UPDATE_AZ_POLICY_TASK_NAME : UPDATE_AZ_POLICY_TASK_NAME + "_" + blockType.name();
      GlobalSingleThreadScheduler.schedule(name, this::updateAzPolicies,
          period, period, TimeUnit.MILLISECONDS);
    }
  }

  private void updateAzPolicies() {
    try {
      byte[] bytes = AzUtils.readFile(filePath);
      String md5Hex = AzUtils.toMD5Hex(bytes);
      if (Objects.equals(md5Hex, this.md5Hex)) {
        LOG.info("The file: {} has not changed, az policy will not be updated", filePath);
        return;
      }
      long ts = System.currentTimeMillis();
      TreeMap<String, AzPolicyInfo> azPolicies = mapper.readValue(new File(filePath),
          typeReference);

      // output historical version of az policy
      if (historicalVersionsPath != null) {
        File dir = new File(historicalVersionsPath);
        if (dir.exists() && dir.isFile()) {
          throw new IllegalStateException(historicalVersionsPath + " must be a directory");
        }
        dir.mkdirs();

        // rename current az policy to previous
        File currentAzPolicy = new File(
            historicalVersionsPath + File.separator + CURRENT_AZ_POLICY);
        if (currentAzPolicy.exists()) {
          currentAzPolicy.renameTo(new File(
              historicalVersionsPath + File.separator + String.format(
                  PREVIOUS_AZ_POLICY_FILE_NAME_FORMAT, ts)));
        }
        // output current az policy
        byte[] json = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(azPolicies);
        AzUtils.outputFile(currentAzPolicy.getPath(), json);
      }
      // replace az policy
      this.md5Hex = md5Hex;
      this.azPolicies = azPolicies;
      LOG.info("Update az policy success from file: " + filePath);
    } catch (Exception e) {
      LOG.info("Can not update az policy", e);
      throw new IllegalStateException(e);
    }
  }

}

