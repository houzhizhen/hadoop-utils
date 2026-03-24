package com.baidu.fs.raw;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

import static com.baidu.fs.raw.Utils.parseTime;
import static org.apache.hadoop.util.StringUtils.formatTime;

public class RepeatGetBlockInfo implements Command {
public static final String NAME = "repeat-getblockinfo";

@Override
public void exec(String[] args) throws IOException {
    Parameters p = new Parameters(args);

    if (!p.has("hdfs-path") || !p.has("size") || !p.has("time")) {
        printUsage();
        return;
    }

    String hdfsPath = p.get("hdfs-path");
    long fileSize = Utils.parseSize(p.get("size"));
    long runTimeMillis = parseTime(p.get("time"));

    System.out.println("开始重复获取块信息测试:");
    System.out.println("文件路径: " + hdfsPath);
    System.out.println("文件大小: " + Utils.formatSize(fileSize));
    System.out.println("运行时间: " + formatTime(runTimeMillis));

    URI uri = URI.create(hdfsPath);
    Path path = new Path(uri.getPath());
    Configuration conf = new HdfsConfiguration();
    DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(uri, conf);

    // 创建测试文件
    Utils.createTestFile(fs, path, fileSize);

    long startTime = System.currentTimeMillis();
    long endTime = startTime + runTimeMillis;
    long iteration = 0;
    long maxGetBlockTime = 0;
    long maxGetBlockIteration = 0;
    long maxGetBlockTimeStart = 0;

    try {
        while (System.currentTimeMillis() < endTime) {
            iteration++;
            long getBlockStartTime = System.currentTimeMillis();

            // 获取块信息
            BlockLocation[] blocks = fs.getFileBlockLocations(path, 0, fileSize);
            long getBlockTime = System.currentTimeMillis() - getBlockStartTime;

            // 记录最长获取时间
            if (getBlockTime > maxGetBlockTime) {
                maxGetBlockTime = getBlockTime;
                maxGetBlockIteration = iteration;
                maxGetBlockTimeStart = getBlockStartTime;
            }

            if (iteration % 10000 == 0) {
                System.out.println("已完成 " + iteration + " 次获取，最大耗时: " + maxGetBlockTime + "ms");
                System.out.println("当前块数量: " + blocks.length);
            }
        }
    } finally {
        fs.close();
    }

    // 输出统计信息
    printStatistics(iteration, maxGetBlockTime, maxGetBlockIteration, maxGetBlockTimeStart);
}

private static void printUsage() {
    System.out.println("parameters: \n");
    System.out.println("--hdfs-path: The path of file \n");
    System.out.println("--size: The size in bytes of file \n");
    System.out.println("--time: The time interval to execute. s:second, m: minute, h:hour  \n");
}

private void printStatistics(long iterations, long maxGetBlockTime,
                             long maxGetBlockIteration, long maxGetBlockTimeStart) {
    System.out.println("\n============== 测试统计 ==============");
    System.out.println("总获取次数: " + iterations);
    System.out.println("最长单次获取时间: " + formatTime(maxGetBlockTime));
    System.out.println("最长获取发生在第 " + maxGetBlockIteration + " 次");
    System.out.println("最长获取开始时间: " + new java.util.Date(maxGetBlockTimeStart));
}

public static void main(String[] args) throws IOException {
    RepeatGetBlockInfo command = new RepeatGetBlockInfo();
    command.exec(args);
}
}