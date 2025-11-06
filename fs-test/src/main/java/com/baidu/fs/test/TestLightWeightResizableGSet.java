package com.baidu.fs.test;

import com.baidu.fs.util.JvmUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.LightWeightResizableGSet;

import java.io.File;

public class TestLightWeightResizableGSet {
public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
    long n = Long.parseLong(args[0]);

    LightWeightResizableGSet<Block, ReplicaInfo> blockSet = new LightWeightResizableGSet<>();
    System.out.println("total insert blocks =" + n);
    System.out.println("=== 插入性能测试 ===");
    long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    insert(blockSet, n);
    long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    System.out.println("内存占用增加: " + (memAfter - memBefore) / (1024 * 1024) + " MB");
}

private static void insert(LightWeightGSet<Block, ReplicaInfo> blockSet, long n) {
    long printInterval = n / 10;
    for (long i = 0; i < n; i += printInterval) {
        long startGcTime = JvmUtils.getCurrentGcTime();
        long beginTime = System.currentTimeMillis();
        insertBatch(i, i+printInterval, blockSet);

        long endTime = System.currentTimeMillis();
        long endGcTime = JvmUtils.getCurrentGcTime();
        long gcTime = endGcTime - startGcTime;
        long timeUsed = endTime - beginTime;
        long timeWithoutGc = timeUsed - (endGcTime - startGcTime);
        System.out.println("Insert " + printInterval + " from " + i + " use " + timeUsed + "ms, gc time " +
            gcTime + "ms, time without gc " + timeWithoutGc + "ms");

    }

}

private static void insertBatch(long from, long to, LightWeightGSet<Block, ReplicaInfo> blockSet) {
    for (long i = from; i < to; i++) {
        FinalizedReplica finalizedReplica = new FinalizedReplica(i, i, i * 10, null, new File("d"+(i % 65536)));
        blockSet.put(finalizedReplica);
    }
}
}
