package org.apache.hadoop.hdfs.server.blockmanagement.az;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

public class AzPolicyInfo {

  public static final AzPolicyInfo DEFAULT = new AzPolicyInfo("", null, false, false, false);

  private final String azPolicy;
  private final String mainAz;
  private final boolean nativeWrite;
  private final boolean disableRedundantDelete;
  private final boolean disableBlockRecovery;
  @JsonIgnore
  private final String[] azPolicyArray;

  @JsonCreator
  public AzPolicyInfo(
      @JsonProperty("azPolicy") String azPolicy,
      @JsonProperty("mainAz") @Nullable String mainAz,
      @JsonProperty(value = "nativeWrite") boolean nativeWrite,
      @JsonProperty(value = "disableRedundantDelete") boolean disableRedundantDelete,
      @JsonProperty(value = "disableBlockRecovery") boolean disableBlockRecovery) {
    this.azPolicy = azPolicy;
    this.mainAz = mainAz;
    this.azPolicyArray = AzUtils.parseAzPolicy(azPolicy);
    this.nativeWrite = nativeWrite;
    this.disableRedundantDelete = disableRedundantDelete;
    this.disableBlockRecovery = disableBlockRecovery;
  }

  public String getAzPolicy() {
    return azPolicy;
  }

  public String getMainAz() {
    return mainAz;
  }

  public boolean isNativeWrite() {
    return nativeWrite;
  }

  public boolean isDisableRedundantDelete() {
    return disableRedundantDelete;
  }

  public boolean isDisableBlockRecovery() {
    return disableBlockRecovery;
  }

  @JsonIgnore
  public String[] getAzPolicyArray() {
    return azPolicyArray;
  }
}

