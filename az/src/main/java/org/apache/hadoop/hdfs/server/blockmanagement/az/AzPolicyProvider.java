package org.apache.hadoop.hdfs.server.blockmanagement.az;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockType;

import java.util.Map.Entry;
import java.util.TreeMap;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzUtils.isParentEntry;

public abstract class AzPolicyProvider {

  protected TreeMap<String, AzPolicyInfo> azPolicies;

  public void initialize(Configuration conf, BlockType blockType) {
    this.azPolicies = new TreeMap<>();
  }

  /**
   * see {@link org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver#findDeepest(java.lang.String)}
   *
   * @param srcPath
   * @return
   */
  public AzPolicyInfo getAzPolicyInfo(String srcPath) {
    Entry<String, AzPolicyInfo> entry = this.azPolicies.floorEntry(srcPath);
    while (entry != null && !isParentEntry(srcPath, entry.getKey())) {
      entry = this.azPolicies.lowerEntry(entry.getKey());
    }
    if (entry == null) {
      return AzPolicyInfo.DEFAULT;
    }
    return entry.getValue();
  }
}
