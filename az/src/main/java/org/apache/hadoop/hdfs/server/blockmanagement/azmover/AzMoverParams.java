package org.apache.hadoop.hdfs.server.azmover;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.azmover.FileStatusVisitor.FileStatusFilter;

@InterfaceAudience.Private
final class AzMoverParams {

  private final String path;
  private final String input;
  private final int targetRep;
  private final int parallelism;
  private final String nameService;
  private final Set<String> excludedPaths;
  private final int listeningThread;
  private final Set<FileStatusFilter> filters;
  private final Map<String, Integer> nodeSelector;
  private final List<String> matchAzPolicy;
  private final boolean skipConfigCheck;

  private AzMoverParams(
      String path,
      String input,
      int targetRep,
      int parallelism,
      String nameService,
      Set<String> excludedPaths,
      int listeningThread,
      Set<FileStatusFilter> filters,
      Map<String, Integer> nodeSelector,
      List<String> matchAzPolicy,
      boolean skipConfigCheck) {
    this.path = path;
    this.input = input;
    this.targetRep = targetRep;
    this.parallelism = parallelism;
    this.nameService = nameService;
    this.excludedPaths = excludedPaths;
    this.listeningThread = listeningThread;
    this.filters = filters;
    this.nodeSelector = nodeSelector;
    this.matchAzPolicy = matchAzPolicy;
    this.skipConfigCheck = skipConfigCheck;
  }

  public String getPath() {
    return path;
  }

  public String getInput() {
    return input;
  }

  public int getTargetRep() {
    return targetRep;
  }

  public int getParallelism() {
    return parallelism;
  }

  public String getNameService() {
    return nameService;
  }

  public Set<String> getExcludedPaths() {
    return excludedPaths;
  }

  public int getListeningThread() {
    return listeningThread;
  }

  public Map<String, Integer> getNodeSelector() {
    return nodeSelector;
  }

  public Set<FileStatusFilter> getFilters() {
    return filters;
  }

  public List<String> getMatchAzPolicy() {
    return matchAzPolicy;
  }

  public boolean isSkipConfigCheck() {
    return skipConfigCheck;
  }

  public static Builder builder() {
    return new Builder();
  }


  public static final class Builder {

    private String path;
    private String input;
    private int targetRep;
    private int parallelism = 1;
    private String nameService;
    private Set<String> excludedPaths = ImmutableSet.of();
    private int listeningThread = 1;
    private Set<FileStatusFilter> filters = ImmutableSet.of();
    private Map<String, Integer> nodeSelector = ImmutableMap.of();
    private List<String> matchAzPolicy = ImmutableList.of();
    private boolean skipConfigCheck;

    private Builder() {
    }

    public Builder withPath(String path) {
      this.path = path;
      return this;
    }

    public Builder withInput(String input) {
      this.input = input;
      return this;
    }

    public Builder withTargetRep(int targetRep) {
      this.targetRep = targetRep;
      return this;
    }

    public Builder withParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder withNameService(String nameService) {
      this.nameService = nameService;
      return this;
    }

    public Builder withExcludedPaths(Set<String> excludedPaths) {
      this.excludedPaths = excludedPaths;
      return this;
    }

    public Builder withListeningThread(int listeningThread) {
      this.listeningThread = listeningThread;
      return this;
    }

    public Builder withFilters(Set<FileStatusFilter> filters) {
      this.filters = filters;
      return this;
    }

    public Builder withNodeSelector(Map<String, Integer> nodeSelector) {
      this.nodeSelector = nodeSelector;
      return this;
    }

    public Builder withMatchAzPolicy(List<String> matchAzPolicy) {
      this.matchAzPolicy = matchAzPolicy;
      return this;
    }

    public Builder skipConfigCheck() {
      this.skipConfigCheck = true;
      return this;
    }

    public AzMoverParams build() {
      Preconditions.checkArgument(path != null || input != null,
          "Both of path and input can not all be null");
      Preconditions.checkNotNull(nameService, "nameService can not be null");
      Preconditions.checkArgument(targetRep > 0, "targetRep must be greater than than 0");
      Preconditions.checkArgument(parallelism > 0, "parallelism must be greater than 0");
      Preconditions.checkArgument(listeningThread > 0, "listeningThread must be greater than 0");
      Preconditions.checkNotNull(excludedPaths, "excludedPaths can not be null");
      Preconditions.checkNotNull(filters, "filters can not be null");
      Preconditions.checkNotNull(nodeSelector, "nodeSelector can not be null");
      Preconditions.checkNotNull(matchAzPolicy, "matchAzPolicy can not be null");
      return new AzMoverParams(
          path,
          input,
          targetRep,
          parallelism,
          nameService,
          excludedPaths,
          listeningThread,
          filters,
          nodeSelector,
          matchAzPolicy,
          skipConfigCheck);
    }
  }

  @Override
  public String toString() {
    return "path=" + path +
        ",\n input=" + input +
        ",\n targetRep=" + targetRep +
        ",\n parallelism=" + parallelism +
        ",\n nameService=" + nameService +
        ",\n excludedPaths=" + excludedPaths +
        ",\n listeningThread=" + listeningThread +
        ",\n " + stringifyFilters() +
        ",\n " + stringifyNodeSelector() +
        ",\n matchAzPolicy=" + matchAzPolicy;
  }

  private String stringifyFilters() {
    return String.format("filters=[%s]", filters.stream()
        .map(FileStatusFilter::description)
        .collect(Collectors.joining(",")));
  }

  private String stringifyNodeSelector() {
    return String.format("nodeSelector=[%s]", nodeSelector.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(",")));
  }
}

