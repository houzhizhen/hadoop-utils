package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzPolicyInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzPolicyProvider;
import org.apache.hadoop.hdfs.server.blockmanagement.az.AzUtils;
import org.apache.hadoop.hdfs.server.blockmanagement.az.LocalFileAzPolicyProvider;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.AZ_POLICY_PROVIDER_IMPL_KEY;
import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzUtils.getAzFromRackLocation;

// TODO: 支持异构存储

/**
 * 这里 rack 的配置是一台 datanode 一个 rack，我们不再感知 rack
 */
public abstract class BlockPlacementPolicyAvailableZone extends AvailableSpaceBlockPlacementPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      BlockPlacementPolicyAvailableZone.class);

  /**
   * Hadoop 3.3.6 的 chooseTargetInOrder 和 chooseReplicasToDelete 不接受 srcPath 参数，
   * 这里通过 ThreadLocal 把 srcPath 从 chooseTarget 传到下游方法。
   */
  private static final ThreadLocal<String> CURRENT_SRC = new ThreadLocal<>();

  /**
   * 自己实现"双选优"逻辑（不用父类的 select，因为 private 进不去）。
   * 复用父类 protected 的 compareDataNode 来比较两个节点。
   */
  private static final Random RAND = new Random();

  private AzPolicyProvider azPolicyProvider;

  @Override
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap,
      Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    Class<? extends AzPolicyProvider> azPolicyProviderClass = conf.getClass(
        AZ_POLICY_PROVIDER_IMPL_KEY, LocalFileAzPolicyProvider.class, AzPolicyProvider.class);
    this.azPolicyProvider = ReflectionUtils.newInstance(azPolicyProviderClass, conf);
    azPolicyProvider.initialize(conf, this.getBlockType());
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode, StorageType type) {
    DatanodeDescriptor a = (DatanodeDescriptor) ((DFSNetworkTopology) clusterMap)
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    DatanodeDescriptor b = (DatanodeDescriptor) ((DFSNetworkTopology) clusterMap)
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    return select(a, b);
  }

  /**
   * 双选优：复用父类的 compareDataNode。compareDataNode == 0 时 a, b 等价；
   * 否则按 50% 概率选剩余空间多的那个，避免完全偏向单个节点造成集中。
   */
  private DatanodeDescriptor select(DatanodeDescriptor a, DatanodeDescriptor b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    int ret = compareDataNode(a, b, false);
    if (ret == 0) {
      return a;
    }
    DatanodeDescriptor preferred = ret < 0 ? a : b;
    DatanodeDescriptor other = ret < 0 ? b : a;
    return RAND.nextInt(100) < 60 ? preferred : other;
  }

  private DatanodeStorageInfo chooseNodeByAz(String az,
      Set<Node> excludedNodes,
      long blockSize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes,
      boolean ignoreError) throws NotEnoughReplicasException {
    try {
      return chooseRandom(az, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      if (ignoreError) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * 取节点 num 次，最后取到的节点数允许小于 num 个
   */
  private DatanodeStorageInfo chooseNodeByAz(int num,
      String az,
      Set<Node> excludedNodes,
      long blockSize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes,
      boolean ignoreError) throws NotEnoughReplicasException {
    DatanodeStorageInfo datanodeStorageInfo = null;
    for (int i = 1; i <= num; i++) {
      DatanodeStorageInfo storageInfo = chooseNodeByAz(az, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes, ignoreError);
      if (storageInfo != null) {
        datanodeStorageInfo = storageInfo;
      }
    }
    return datanodeStorageInfo;
  }

  /**
   * 按照 result，从 az policy 中排除已选择的 az
   *
   * @param azPolicy az policy
   * @param results  已选择节点
   * @return az 列表
   */
  private String[] selectAz(String[] azPolicy, List<DatanodeStorageInfo> results) {
    Map<String, Integer> azCounter = new HashMap<>();
    for (DatanodeStorageInfo datanodeStorageInfo : results) {
      String rackLocation = datanodeStorageInfo.getDatanodeDescriptor().getNetworkLocation();
      String az = getAzFromRackLocation(rackLocation);
      azCounter.put(az, azCounter.getOrDefault(az, 0) + 1);
    }
    return AzUtils.selectAz(azPolicy, azCounter);
  }

  private DatanodeStorageInfo chooseOneByAzPolicy(Set<Node> excludedNodes,
      long blockSize, int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes, String[] azPolicy)
      throws NotEnoughReplicasException {
    for (String az : azPolicy) {
      DatanodeStorageInfo datanodeStorageInfo = chooseNodeByAz(az, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes, true);
      if (datanodeStorageInfo != null) {
        return datanodeStorageInfo;
      }
    }
    throw new NotEnoughReplicasException(
        "Can not choose first storage, az policy is: " + Arrays.toString(azPolicy));
  }

  private DatanodeStorageInfo chooseFirstStorage(@Nullable String writerAz,
      Set<Node> excludedNodes, long blockSize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, String[] azPolicy,
      int numOfReplicas) throws NotEnoughReplicasException {
    // 如果 writerAz 为空，则选择 az policy 列表中能拿到的第一个 az
    if (writerAz == null) {
      // 这里如果一个都拿不到，直接报错
      return chooseOneByAzPolicy(excludedNodes, blockSize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes, azPolicy);
    }
    // 如果 writer 不为空，则判断 writer 在不在 az policy 内，
    // 在就按照 writer 的 az 取
    int index = Arrays.binarySearch(azPolicy, writerAz);
    if (index >= 0 && index < numOfReplicas) {
      DatanodeStorageInfo datanodeStorageInfo = chooseNodeByAz(writerAz, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes, true);
      if (datanodeStorageInfo != null) {
        return datanodeStorageInfo;
      }
    }
    // 从 write 的 az 没拿到，则从 az policy 再挑选
    return chooseOneByAzPolicy(excludedNodes, blockSize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes, azPolicy);
  }

  private boolean isSelectFromMainAz(String[] azPolicy) {
    BlockType blockType = getBlockType();
    switch (blockType) {
      case CONTIGUOUS:
        return azPolicy == null || azPolicy.length == 0;
      case STRIPED:
        return true;
      default:
        throw new IllegalStateException("Unsupported block type: " + blockType);
    }
  }

  public DatanodeStorageInfo[] chooseTarget(String srcPath,
      int numOfReplicas,
      Node writer,
      List<DatanodeStorageInfo> chosen,
      boolean returnChosenNodes,
      Set<Node> excludedNodes,
      long blockSize,
      BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {

    if (chosen.size() != 0 && azPolicyProvider.getAzPolicyInfo(srcPath).isDisableBlockRecovery()) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }

    CURRENT_SRC.set(srcPath);
    try {
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosen, returnChosenNodes,
          excludedNodes, blockSize, storagePolicy, flags);
    } finally {
      CURRENT_SRC.remove();
    }
  }

  @Override
  protected Node chooseTargetInOrder(int numOfReplicas,
      Node writer,
      final Set<Node> excludedNodes,
      final long blockSize,
      final int maxNodesPerRack,
      final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes,
      final boolean newBlock,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {

    int targetRep = results.size() + numOfReplicas;

    String srcPath = CURRENT_SRC.get();
    AzPolicyInfo azPolicyInfo = azPolicyProvider.getAzPolicyInfo(srcPath);
    String[] azPolicy = azPolicyInfo.getAzPolicyArray();
    String mainAz = azPolicyInfo.getMainAz();

    String writerAz =
        writer == null ? null : AzUtils.getAzFromRackLocation(writer.getNetworkLocation());

    // 如果开启 native write，则优先从 writer 所在 az 挑选节点，如果 write 为 null，则走正常的 az policy
    if (writerAz != null && azPolicyInfo.isNativeWrite()) {
      chooseNodeByAz(numOfReplicas, writerAz, excludedNodes, blockSize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes, true);
      return writer;
    }

    // 判断是否从 mainAz 挑选节点
    if (isSelectFromMainAz(azPolicy)) {
      if (StringUtils.isEmpty(mainAz)) {
        // 没有配置 mainAz，fallback 到父类
        return super.chooseTargetInOrder(numOfReplicas, writer, excludedNodes, blockSize,
            maxNodesPerRack, results, avoidStaleNodes, newBlock, storageTypes);
      }
      // 从 mainAz 拿
      DatanodeStorageInfo datanodeStorageInfo = chooseNodeByAz(numOfReplicas, mainAz, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes, true);
      if (writer == null) {
        writer = datanodeStorageInfo == null ? null : datanodeStorageInfo.getDatanodeDescriptor();
      }
      return writer;
    }

    // 开始走 az policy 逻辑
    if (results.size() == 0) {
      // 挑选第一个节点，这里强制规定一定要拿到，不然报错
      DatanodeStorageInfo datanodeStorageInfo = chooseFirstStorage(writerAz, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes, azPolicy,
          numOfReplicas);
      if (writer == null) {
        writer = datanodeStorageInfo.getDatanodeDescriptor();
      }
      if (--numOfReplicas == 0) {
        return writer;
      }
    }

    // 按照 result，从 az policy 中排除已选择的 az
    String[] selectedAz = selectAz(azPolicy, results);

    for (int i = 0; i < selectedAz.length; i++) {
      String az = selectedAz[i];
      if (az == null) {
        continue;
      }
      DatanodeStorageInfo storageInfo = chooseNodeByAz(az, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes, true);
      if (storageInfo != null) {
        numOfReplicas--;
        selectedAz[i] = null;
        if (writer == null) {
          writer = storageInfo.getDatanodeDescriptor();
        }
      }
      if (numOfReplicas == 0) {
        // 这里可能不能满足 az policy 但是副本数达到了直接退出
        break;
      }
    }

    // 如果副本没拿够，有两种情况：
    // 1. az policy 满足了，从 main az 拿，没有 main az 不拿了
    // 2. az policy 未满足，不拿了
    if (Arrays.stream(selectedAz).allMatch(Objects::isNull) && StringUtils.isNotEmpty(mainAz)) {
      chooseNodeByAz(numOfReplicas, mainAz, excludedNodes, blockSize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes, true);
    }

    // 检查是否拿到足够的副本，如果不够，抛出异常，走父类重试逻辑，fallback 到不同的存储介质
    if (targetRep != results.size()) {
      throw new NotEnoughReplicasException(
          String.format("Current replication is %s, but target replication is %s",
              results.size(), targetRep));
    }

    // 排序，以第一个节点为准
    AzUtils.sortByAz(results, datanodeStorageInfo -> AzUtils.getAzFromRackLocation(
        datanodeStorageInfo.getDatanodeDescriptor().getNetworkLocation()));
    return writer;
  }

  /**
   * 找到剩余空间最大的 datanode 和心跳最新的 datanode
   */
  private DatanodeStorageInfo findNodeWithNewestHeartbeatOrMaxSpace(
      List<DatanodeStorageInfo> datanodeStorageInfos, List<StorageType> excessTypes) {
    long newestHeartbeat = Long.MIN_VALUE;
    DatanodeStorageInfo newestHeartbeatStorage = null;
    long maxSpace = Long.MIN_VALUE;
    DatanodeStorageInfo maxSpaceStorage = null;

    for (DatanodeStorageInfo datanodeStorageInfo : datanodeStorageInfos) {
      if (!excessTypes.contains(datanodeStorageInfo.getStorageType())) {
        continue;
      }

      final DatanodeDescriptor node = datanodeStorageInfo.getDatanodeDescriptor();
      long free = datanodeStorageInfo.getRemaining();
      long lastHeartbeat = node.getLastUpdateMonotonic();
      if (lastHeartbeat > newestHeartbeat) {
        newestHeartbeat = lastHeartbeat;
        newestHeartbeatStorage = datanodeStorageInfo;
      }
      if (maxSpace < free) {
        maxSpace = free;
        maxSpaceStorage = datanodeStorageInfo;
      }
    }
    // 最新心跳的 datanode 优先排除，其次是最大剩余空间的
    final DatanodeStorageInfo storage;
    if (newestHeartbeatStorage != null) {
      storage = newestHeartbeatStorage;
    } else {
      storage = maxSpaceStorage;
    }
    return storage;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates, int expectedNumOfReplicas,
      List<StorageType> excessTypes, DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {

    // hadoop 3.3.6 不再传 srcPath，尝试从 ThreadLocal 取；调用栈不来自 chooseTarget 时为 null，
    // 此时 azPolicyProvider 应当返回默认 policy。
    String srcPath = CURRENT_SRC.get();
    AzPolicyInfo azPolicyInfo = srcPath == null
        ? AzPolicyInfo.DEFAULT
        : azPolicyProvider.getAzPolicyInfo(srcPath);

    // 这个配置是迁移专用，禁用删除多余副本，先拷贝 block 再设置 replication
    if (azPolicyInfo.isDisableRedundantDelete()) {
      return ImmutableList.of();
    }

    String[] azPolicy = azPolicyInfo.getAzPolicyArray();
    // fall back
    if (getBlockType().equals(BlockType.STRIPED) || azPolicy == null || azPolicy.length == 0) {
      return super.chooseReplicasToDelete(availableReplicas, delCandidates,
          expectedNumOfReplicas, excessTypes, addedNode, delNodeHint);
    }

    List<DatanodeStorageInfo> delCandidateCopy = new ArrayList<>(delCandidates.size());
    List<StorageType> excessTypesCopy = new ArrayList<>(excessTypes);

    Map<String, List<DatanodeStorageInfo>> azToNodes = new HashMap<>();
    for (DatanodeStorageInfo datanodeStorageInfo : delCandidates) {
      String rack = datanodeStorageInfo.getDatanodeDescriptor().getNetworkLocation();
      String az = getAzFromRackLocation(rack);
      List<DatanodeStorageInfo> datanodeStorageInfos = azToNodes.computeIfAbsent(az,
          k -> new ArrayList<>());
      datanodeStorageInfos.add(datanodeStorageInfo);
      delCandidateCopy.add(datanodeStorageInfo);
    }

    // 检查 delHint 是否符合需求
    DatanodeStorageInfo delHintDatanodeStorageInfo = DatanodeStorageInfo.getDatanodeStorageInfo(
        delCandidates, delNodeHint);
    if (delHintDatanodeStorageInfo != null && excessTypesCopy.remove(
        delHintDatanodeStorageInfo.getStorageType())) {
      // 暂时移除，后面再加回来
      delCandidateCopy.remove(delHintDatanodeStorageInfo);
    } else {
      delHintDatanodeStorageInfo = null;
    }

    // 从 delCandidate 中移除 az policy 对应的节点，不删除这些节点
    for (int i = 0; i < azPolicy.length && expectedNumOfReplicas > 0; i++) {
      String az = azPolicy[i];
      List<DatanodeStorageInfo> datanodeStorageInfos = azToNodes.get(az);
      if (datanodeStorageInfos == null || datanodeStorageInfos.size() == 0) {
        continue;
      }
      // 优先保留最活跃的节点和存储空间剩余最大节点
      DatanodeStorageInfo node = findNodeWithNewestHeartbeatOrMaxSpace(datanodeStorageInfos,
          excessTypesCopy);
      if (node != null) {
        delCandidateCopy.remove(node);
        datanodeStorageInfos.remove(node);
        expectedNumOfReplicas--;
      }
    }

    List<DatanodeStorageInfo> result = delCandidateCopy;
    if (expectedNumOfReplicas != 0) {
      result = super.chooseReplicasToDelete(
          availableReplicas, delCandidates, expectedNumOfReplicas, excessTypesCopy, addedNode,
          null);
    }
    if (delHintDatanodeStorageInfo != null) {
      result.add(delHintDatanodeStorageInfo);
    }
    return result;
  }


  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs, DatanodeInfo source,
      DatanodeInfo target) {
    // TODO: 可能需要支持更多的场景或配置，让 mover 和 balancer 来支持暂时的跨机房
    // 在一个机房的才能互相移动
    return getAzFromRackLocation(source.getNetworkLocation()).equals(
        getAzFromRackLocation(target.getNetworkLocation()));
  }

  protected abstract BlockType getBlockType();

}

