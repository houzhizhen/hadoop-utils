package org.apache.hadoop.hdfs.server.blockmanagement.az;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.net.DNSToSwitchMapping;

public class LocalFileRawMapping implements DNSToSwitchMapping {

  private Map<String, String> host2rackMap;

  @Override
  public List<String> resolve(List<String> names) {
    if (names == null) {
      return null;
    }
    List<String> result = names.stream().map(host2rackMap::get).filter(Objects::nonNull)
        .collect(Collectors.toList());
    if (result.size() == 0 || names.size() != result.size()) {
      return null;
    }
    return result;
  }

  @Override
  public void reloadCachedMappings() {

  }

  @Override
  public void reloadCachedMappings(List<String> names) {

  }

  public void setHost2rackMap(Map<String, String> host2rackMap) {
    this.host2rackMap = host2rackMap;
  }
}

