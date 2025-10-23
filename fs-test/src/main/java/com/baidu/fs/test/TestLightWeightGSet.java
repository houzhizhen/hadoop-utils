package com.baidu.fs.test;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.util.LightWeightGSet;

public class TestLightWeightGSet {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        int n = Integer.parseInt(args[0]);
//        int n = 100_000_000;             // 测试块数量,1亿
        double percentage = 2.0D;        // entries 大小，默认2%
        int capacity = LightWeightGSet.computeCapacity(percentage, "BlocksMap");
        LightWeightGSet<BlockInfo, BlockInfo> blockSet = new LightWeightGSet<>(capacity);
        System.out.println("total blocks =" + n);
        System.out.println("=== 插入性能测试 ===");
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long start = System.currentTimeMillis();
        {
            int printInterval = n / 10;
            int nextPrintAt = printInterval;
            long printTimeBegin = start;
            for (int i = 0; i < n; i++) {
                Block block = new Block(i);
                BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 3);
                blockSet.put(blockInfo);
                if (i == nextPrintAt) {
                    long currentTime = System.currentTimeMillis();
                    long printTime = currentTime - printTimeBegin;
                    System.out.println("Insert next " + printInterval + " of " + n + " using " + printTime + "ms");
                    printTimeBegin = currentTime;
                    nextPrintAt += printInterval;
                }
            }
        }

        long end = System.currentTimeMillis();
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("插入 " + n + " 个 BlockInfo 耗时: " + (end - start) + " ms");
        System.out.println("内存占用增加: " + (memAfter - memBefore) / (1024 * 1024) + " MB");

        System.out.println("\n=== 查询性能测试 ===");
        start = System.nanoTime();
        {
            int printInterval = n / 10;
            int nextPrintAt = printInterval;
            long printTimeBegin = start;
            for (int i = 0; i < n; i++) {
                BlockInfo key = new BlockInfoContiguous(new Block(i), (short) 3);
                BlockInfo result = blockSet.get(key);
                if (result == null) {
                    System.out.println("BlockInfo 未找到: " + i);
                }
                if (i == nextPrintAt) {
                    long currentTime = System.currentTimeMillis();
                    long printTime = currentTime - printTimeBegin;
                    System.out.println("Get next " + printInterval + " of " + n + " using " + printTime + "ms");
                    printTimeBegin = currentTime;
                    nextPrintAt += printInterval;
                }
            }
        }
        end = System.nanoTime();
        System.out.println("平均每次查询耗时: " + ((end - start) / n) + " ns");

        System.out.println("\n=== 删除性能测试 ===");
        start = System.currentTimeMillis();
        {
            int printInterval = n / 10;
            int nextPrintAt = printInterval;
            long printTimeBegin = start;
            for (int i = 0; i < n; i++) {
                BlockInfo key = new BlockInfoContiguous(new Block(i), (short) 3);
                blockSet.remove(key);
                if (i == nextPrintAt) {
                    long currentTime = System.currentTimeMillis();
                    long printTime = currentTime - printTimeBegin;
                    System.out.println("Delete next " + printInterval + " of " + n + " using " + printTime + "ms");
                    printTimeBegin = currentTime;
                    nextPrintAt += printInterval;
                }
            }

        }
        end = System.currentTimeMillis();
        System.out.println("删除 " + n + " 个 BlockInfo 耗时: " + (end - start) + " ms");

    }
}
