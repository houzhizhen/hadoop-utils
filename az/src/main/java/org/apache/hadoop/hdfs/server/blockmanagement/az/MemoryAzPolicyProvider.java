package org.apache.hadoop.hdfs.server.blockmanagement.az;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockType;

import java.util.TreeMap;

/**
 * 内存存储，测试专用
 */
public class MemoryAzPolicyProvider extends AzPolicyProvider {

  public static final TreeMap<String, AzPolicyInfo> globalAzPolicies = new TreeMap<>();

  @Override
  public void initialize(Configuration conf, BlockType blockType) {
    super.initialize(conf, blockType);
    this.azPolicies = globalAzPolicies;
  }

  public static void clear() {
    globalAzPolicies.clear();
  }

  public static synchronized void addPolicy(String srcPath, AzPolicyInfo azPolicyInfo) {
    globalAzPolicies.put(srcPath, azPolicyInfo);
  }

  public static synchronized void addPolicy(String srcPath, String azPolicy) {
    globalAzPolicies.put(srcPath, new AzPolicyInfo(azPolicy, null, false, false, false));
  }

  public static synchronized void addPolicy(String srcPath, String[] azPolicy) {
    addPolicy(srcPath, azPolicy, false);
  }

  public static synchronized void addPolicy(String srcPath, String[] azPolicy,
      boolean nativeWrite) {
    globalAzPolicies.put(srcPath, new AzPolicyInfo(String.join(",", azPolicy), null, nativeWrite,
        false, false));
  }

  public static synchronized void removePolicy(String srcPath) {
    globalAzPolicies.remove(srcPath);
  }

}
