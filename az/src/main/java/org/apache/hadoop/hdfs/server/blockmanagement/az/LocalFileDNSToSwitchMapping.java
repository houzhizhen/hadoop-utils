package org.apache.hadoop.hdfs.server.blockmanagement.az;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_LOCAL_FILE_PATH_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DNS_SWITCH_MAPPING_LOCAL_FILE_PATH_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DNS_SWITCH_MAPPING_LOCAL_FILE_UPDATE_PERIOD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.DNS_SWITCH_MAPPING_LOCAL_FILE_UPDATE_PERIOD_MS_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.UPDATE_DNS_MAPPING_TASK_NAME;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileDNSToSwitchMapping extends CachedDNSToSwitchMapping {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileDNSToSwitchMapping.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final TypeReference<HashMap<String, String>> typeReference = new TypeReference<HashMap<String, String>>() {
  };

  private String filePath;
  private String md5Hex;


  public LocalFileDNSToSwitchMapping() {
    super(new LocalFileRawMapping());
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.filePath = Preconditions.checkNotNull(conf.get(DNS_SWITCH_MAPPING_LOCAL_FILE_PATH_KEY),
        "%s can not be null", AZ_POLICY_PROVIDER_LOCAL_FILE_PATH_KEY);
    updateCache();
    addUpdateDnsMappingTask(conf);
  }

  private void updateCache() {
    try {
      byte[] bytes = AzUtils.readFile(filePath);
      String md5Hex = AzUtils.toMD5Hex(bytes);
      if (!md5Hex.equals(this.md5Hex)) {
        this.md5Hex = md5Hex;
        Map<String, String> map = mapper.readValue(new File(filePath), typeReference);
        Map<String, String> switchMap = getSwitchMap();
        List<String> diffKeys = AzUtils.diffMap(map, switchMap);
        ((LocalFileRawMapping) rawMapping).setHost2rackMap(map);
        reloadCachedMappings(diffKeys);
        LOG.info("Update dns mapping success from file: " + filePath);
      } else {
        LOG.info("The file: {} has not changed, dns mapping will not be updated", filePath);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void addUpdateDnsMappingTask(Configuration conf) {
    long period = conf.getLong(DNS_SWITCH_MAPPING_LOCAL_FILE_UPDATE_PERIOD_MS_KEY,
        DNS_SWITCH_MAPPING_LOCAL_FILE_UPDATE_PERIOD_MS_DEFAULT);
    if (period > 0) {
      GlobalSingleThreadScheduler.schedule(UPDATE_DNS_MAPPING_TASK_NAME, this::updateCache, period,
          period, TimeUnit.MILLISECONDS);
    }
  }
}

