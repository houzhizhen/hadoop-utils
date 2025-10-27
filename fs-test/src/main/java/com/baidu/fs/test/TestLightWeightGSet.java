package com.baidu.fs.test;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.util.LightWeightGSet;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class TestLightWeightGSet {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        long n = Long.parseLong(args[0]);
//        int n = 100_000_000;             // 测试块数量,1亿
        double percentage = 2.0D;        // entries 大小，默认2%
        int capacity = LightWeightGSet.computeCapacity(percentage, "BlocksMap");
        LightWeightGSet<BlockInfo, BlockInfo> blockSet = new LightWeightGSet<>(capacity);
        System.out.println("total insert blocks =" + n);
        System.out.println("=== 插入性能测试 ===");
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        insert(blockSet, n);
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("内存占用增加: " + (memAfter - memBefore) / (1024 * 1024) + " MB");
    }

    private static void insert(LightWeightGSet<BlockInfo, BlockInfo> blockSet, long n) {

            long printInterval = n / 10;
            for (long i = 0; i < n; i += printInterval) {
                long startGcTime = getCurrentGcTime();
                long beginTime = System.currentTimeMillis();
                insertBatch(i, i+printInterval, blockSet);

                long endTime = System.currentTimeMillis();
                long endGcTime = getCurrentGcTime();
                long gcTime = endGcTime - startGcTime;
                long timeUsed = endTime - beginTime;
                long timeWithoutGc = timeUsed - (endGcTime - startGcTime);
                System.out.println("Insert " + printInterval + " from " + i + " use " + timeUsed + "ms, gc time " +
                    gcTime + "ms, time without gc " + timeWithoutGc + "ms");

            }

    }

    private static void insertBatch(long from, long to, LightWeightGSet<BlockInfo, BlockInfo> blockSet) {
        for (long i = from; i < to; i++) {
            Block block = new Block(i);
            BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 3);
            blockSet.put(blockInfo);
        }
    }

    public static long getCurrentGcTime() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        long totalGcTime = 0L;
        for (GarbageCollectorMXBean bean : gcBeans) {

            totalGcTime += bean.getCollectionTime();
        }
        return totalGcTime;
    }

}
