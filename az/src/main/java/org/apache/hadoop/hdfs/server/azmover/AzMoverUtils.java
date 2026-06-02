package org.apache.hadoop.hdfs.server.azmover;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzPolicyInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzPolicyProvider;

public class AzMoverUtils {

  protected static List<String> getAzPolicyForTargetRep(Path path,
      AzPolicyProvider azPolicyProvider, int targetRep) {
    AzPolicyInfo azPolicyInfo = azPolicyProvider.getAzPolicyInfo(getPathWithoutPrefix(path));
    String mainAz = Objects.requireNonNull(azPolicyInfo.getMainAz(), "Main az can not be null");
    String[] azPolicyArray = azPolicyInfo.getAzPolicyArray();
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (int i = 0; i < targetRep; i++) {
      if (i <= azPolicyArray.length - 1) {
        builder.add(azPolicyArray[i]);
      } else {
        builder.add(mainAz);
      }
    }
    return builder.build();
  }


  public static String getPathWithoutPrefix(Path path) {
    return path.toString().replaceFirst("hdfs://[^/]*", "");
  }

  public static Pattern FQDN_PREFIX_PATTERN = Pattern.compile("[a-zA-Z_\\-]*(\\d+)");

  public static int getFqdnPrefixNumber(String hostname) {
    Matcher matcher = FQDN_PREFIX_PATTERN.matcher(hostname.split("\\.")[0]);
    if (!matcher.find()) {
      return Integer.MAX_VALUE;
    }
    return Integer.parseInt(matcher.group(1));
  }

  public static String getFqdnSuffix(String hostname) {
    int index = hostname.indexOf('.');
    return index < 0 ? Objects.toString(null) : hostname.substring(index + 1);
  }

  public static Comparator<String> getFqdnComparator() {
    return Comparator.comparing(AzMoverUtils::getFqdnSuffix)
        .thenComparing(AzMoverUtils::getFqdnPrefixNumber);
  }

  @VisibleForTesting
  public static boolean azPolicyMatch(List<String> matchAzPolicy, List<String> currentAz) {
    if (matchAzPolicy.size() != currentAz.size()) {
      return false;
    }
    LinkedList<String> list = new LinkedList<>(matchAzPolicy);
    currentAz.forEach(list::remove);
    return list.size() == 0;
  }

  public static DatanodeInfo removeMinRemainingSpaceDatanode(List<DatanodeInfo> datanodeInfos) {
    int index = 0;
    for (int i = 1; i < datanodeInfos.size(); i++) {
      long minRemaining = datanodeInfos.get(index).getRemaining();
      long remaining = datanodeInfos.get(i).getRemaining();
      if (remaining < minRemaining) {
        index = i;
      }
    }
    return datanodeInfos.remove(index);
  }
}

