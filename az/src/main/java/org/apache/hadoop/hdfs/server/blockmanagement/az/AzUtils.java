package org.apache.hadoop.hdfs.server.blockmanagement.az;

import static org.apache.hadoop.hdfs.server.blockmanagement.az.AzConstant.EMPTY_STRING_ARRAY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.io.IOUtils;

public class AzUtils {

  public static boolean isParentEntry(final String path, final String parent) {
    if (!path.startsWith(parent)) {
      return false;
    }

    if (path.equals(parent)) {
      return true;
    }

    return path.charAt(parent.length()) == Path.SEPARATOR_CHAR
        || parent.equals(Path.SEPARATOR);
  }

  /**
   * @param azPolicy  az policy
   * @param azCounter 已选取的节点计数
   * @return 剩余的节点所在 az 列表
   */
  @VisibleForTesting
  public static String[] selectAz(String[] azPolicy, Map<String, Integer> azCounter) {
    String[] selectedAz = Arrays.copyOf(azPolicy, azPolicy.length);
    for (int i = 0; i < azPolicy.length; i++) {
      String az = azPolicy[i];
      Integer count = azCounter.remove(az);
      if (count != null && --count >= 0) {
        selectedAz[i] = null;
        if (count > 0) {
          azCounter.put(az, count);
        }
      }
    }
    return selectedAz;
  }

  /**
   * 排序规则是第一个节点不动，将与第一个节点 az 相同的放在前面，其他的正常排序
   */
  public static <T> void sortByAz(List<T> results, Function<T,String> azMapping) {
    if (results.size() <= 2) {
      return;
    }
    String localAz = azMapping.apply(results.get(0));
    results.subList(1, results.size()).sort((o1, o2) -> {
      String az1 = azMapping.apply(o1);
      String az2 = azMapping.apply(o2);
      if (az1.equals(az2)) {
        return 0;
      }
      if (localAz.equals(az1)) {
        return -1;
      }
      if (localAz.equals(az2)) {
        return 1;
      }
      return az1.compareTo(az2);
    });
  }

  // az policy 的格式为 /az1,/az2,/az1,/az1,/az2
  public static String[] parseAzPolicy(String policyString) {
    if (StringUtils.isEmpty(policyString)) {
      return EMPTY_STRING_ARRAY;
    }
    return policyString.split(",");
  }

  public static final String UNKNOWN_AZ_PREFIX = "UNKNOWN_AZ-";

  public static String randomUnknownAzName() {
    return UNKNOWN_AZ_PREFIX + UUID.randomUUID();
  }

  public static String getAzFromRackLocation(String rackLocation) {
    if (StringUtils.isEmpty(rackLocation)) {
      return randomUnknownAzName();
    }
    int lastIndex = rackLocation.lastIndexOf(Path.SEPARATOR);
    if (lastIndex < 0) {
      return randomUnknownAzName();
    }
    return rackLocation.substring(0, lastIndex);
  }


  public static File checkFile(String filePath) {
    File file = new File(filePath);
    Preconditions.checkState(file.exists(), filePath + " does not exist");
    Preconditions.checkState(file.isFile(), filePath + " must be file");
    return file;
  }

  public static byte[] readFile(String filePath) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(checkFile(filePath));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(inputStream.available())) {
      IOUtils.copyBytes(inputStream, outputStream, 4096, false);
      return outputStream.toByteArray();
    }
  }

  public static void outputFile(String filePath, byte[] data) throws IOException {
    try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
      outputStream.write(data);
    }
  }

  public static String toMD5Hex(byte[] bytes) {
    return DigestUtils.md5Hex(bytes);
  }


  public static List<String> diffMap(Map<String, String> map1, Map<String, String> map2) {
    List<String> diffKeys = new ArrayList<>();
    map1.keySet().forEach(key -> {
      if (!Objects.equals(map1.get(key), map2.get(key))) {
        diffKeys.add(key);
      }
    });
    map2.keySet().forEach(key -> {
      if (!Objects.equals(map1.get(key), map2.get(key))) {
        diffKeys.add(key);
      }
    });
    return diffKeys;
  }

  private static DatanodeInfo[] filterDatanodeByAz(DatanodeInfo[] datanodeInfos,
      @Nonnull String az) {
    if (datanodeInfos == null) {
      return DatanodeInfo.EMPTY_ARRAY;
    }
    return Arrays.stream(datanodeInfos)
        .filter(datanodeInfo -> getAzFromRackLocation(datanodeInfo.getNetworkLocation()).equals(az))
        .toArray(DatanodeInfo[]::new);
  }

  private static int[] filterDatanodeByAzReturnIndexes(DatanodeInfo[] datanodeInfos,
      @Nonnull String az) {
    if (datanodeInfos == null) {
      return new int[0];
    }
    return IntStream.range(0, datanodeInfos.length)
        .filter(i -> getAzFromRackLocation(datanodeInfos[i].getNetworkLocation()).equals(az))
        .toArray();
  }

  private static LocatedBlock filterLocationByAz(LocatedBlock locatedBlock, @Nonnull String az) {
    DatanodeInfo[] datanodeInfos = locatedBlock.getLocations();
    int[] indexes = filterDatanodeByAzReturnIndexes(datanodeInfos, az);
    if (indexes.length == 0) {
      return locatedBlock;
    }
    String[] storageIDs = locatedBlock.getStorageIDs();
    StorageType[] storageTypes = locatedBlock.getStorageTypes();
    // filter
    DatanodeInfo[] filterDatanodeInfos = Arrays.stream(indexes).mapToObj(i -> datanodeInfos[i])
        .toArray(DatanodeInfo[]::new);
    String[] filterStorageIDs = Arrays.stream(indexes).mapToObj(i -> storageIDs[i])
        .toArray(String[]::new);
    StorageType[] filterStorageTypes = Arrays.stream(indexes).mapToObj(i -> storageTypes[i])
        .toArray(StorageType[]::new);

    DatanodeInfo[] filterCacheLocations = filterDatanodeByAz(locatedBlock.getCachedLocations(), az);

    return new LocatedBlock(
        locatedBlock.getBlock(),
        filterDatanodeInfos,
        filterStorageIDs,
        filterStorageTypes,
        locatedBlock.getStartOffset(),
        locatedBlock.isCorrupt(),
        filterCacheLocations);
  }

  public static LocatedBlocks filterBlocksByAz(LocatedBlocks locatedBlocks, @Nullable String az) {
    if (az == null) {
      return locatedBlocks;
    }
    List<LocatedBlock> locatedBlockList = locatedBlocks.getLocatedBlocks()
        .stream()
        .map(locatedBlock -> filterLocationByAz(locatedBlock, az))
        .collect(Collectors.toList());
    return new LocatedBlocks(
        locatedBlocks.getFileLength(),
        locatedBlocks.isUnderConstruction(),
        locatedBlockList,
        locatedBlocks.getLastLocatedBlock(),
        locatedBlocks.isLastBlockComplete(),
        locatedBlocks.getFileEncryptionInfo(),
        locatedBlocks.getErasureCodingPolicy());
  }

}

