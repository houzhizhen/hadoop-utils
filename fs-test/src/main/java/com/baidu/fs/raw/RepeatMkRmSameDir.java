package com.baidu.fs.raw;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class RepeatMkRmSameDir implements Command {
public static final String NAME = "repeat-mkrm-dir";

@Override
public void exec(String[] args) throws IOException {
    Parameters p = new Parameters(args);

    if (!p.has("hdfs-path") || !p.has("time")) {
        printUsage();
        return;
    }

    String hdfsPath = p.get("hdfs-path");
    long runTimeMillis = Utils.parseTime(p.get("time"));

    System.out.println("开始重复创建删除目录测试:");
    System.out.println("目录路径: " + hdfsPath);
    System.out.println("运行时间: " + Utils.formatTime(runTimeMillis));

    URI uri = URI.create(hdfsPath);
    Path path = new Path(uri.getPath());
    Configuration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);

    long startTime = System.currentTimeMillis();
    long endTime = startTime + runTimeMillis;
    int iteration = 0;
    long maxIterTime = 0;

    try {
        while (System.currentTimeMillis() < endTime) {
            iteration++;
            long iterBegin = System.currentTimeMillis();
            fs.mkdirs(path);
            fs.delete(path, true);

            long iterInterval = System.currentTimeMillis() - iterBegin;
            if (maxIterTime < iterInterval) {
                maxIterTime = iterInterval;
            }
            if (iteration % 1000 == 0) {
                System.out.println("已完成 " + iteration + " 次mkdir+delete，最大耗时：" + maxIterTime + "ms");
            }
        }
    } finally {
        fs.close();
        System.out.println("总次数: " + iteration + ", maxIterTime: " + maxIterTime + "ms");
    }

}

private static void printUsage() {
    System.out.println("parameters: \n");
    System.out.println("--hdfs-path: The path of directory \n");
    System.out.println("--time: The time interval to execute. s:second, m: minute, h:hour  \n");
}


public static void main(String[] args) throws IOException {
    RepeatMkRmSameDir command = new RepeatMkRmSameDir();
    command.exec(args);
}
}
