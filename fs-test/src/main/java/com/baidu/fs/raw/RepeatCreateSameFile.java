package com.baidu.fs.raw;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class RepeatCreateSameFile implements Command {
public static final String NAME = "repeat-create";

@Override
public void exec(String[] args) throws IOException {
    Parameters p = new Parameters(args);

    if (!p.has("hdfs-path") || !p.has("size") || !p.has("time")) {
        printUsage();
        return;
    }

    String hdfsPath = p.get("hdfs-path");
    long fileSize = Utils.parseSize(p.get("size"));
    long runTimeMillis = Utils.parseTime(p.get("time"));
    int bufferSize = p.getInt("buffer-size", 4096);

    System.out.println("开始重复创建文件测试:");
    System.out.println("文件路径: " + hdfsPath);
    System.out.println("文件大小: " + Utils.formatSize(fileSize));
    System.out.println("运行时间: " + Utils.formatTime(runTimeMillis));
    System.out.println("缓冲区大小: " + bufferSize + " bytes");

    URI uri = URI.create(hdfsPath);
    Path path = new Path(uri.getPath());
    Configuration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);

    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < buffer.length; i++) {
        buffer[i] = (byte) (i % 256);
    }

    long startTime = System.currentTimeMillis();
    long endTime = startTime + runTimeMillis;
    int iteration = 0;
    long maxIterTime = 0;

    try {
        while (System.currentTimeMillis() < endTime) {
            iteration++;
            long iterBegin = System.currentTimeMillis();
            OutputStream out = fs.create(path, true);
            long remaining = fileSize;
            while (remaining > 0) {
                int bytesToWrite = (int) Math.min(buffer.length, remaining);
                out.write(buffer, 0, bytesToWrite);
                remaining -= bytesToWrite;
            }
            out.close();

            long iterInterval = System.currentTimeMillis() - iterBegin;
            if (maxIterTime < iterInterval) {
                maxIterTime = iterInterval;
            }
            if (iteration % 1000 == 0) {
                System.out.println("已完成 " + iteration + " 次写入，最大耗时：" + maxIterTime + "ms");
            }
        }
    } finally {
        fs.close();
        System.out.println("maxIterTime: " + maxIterTime + "ms");
    }

}

private static void printUsage() {
    System.out.println("parameters: \n");
    System.out.println("--hdfs-path: The path of file \n");

    System.out.println("--size: The size in bytes of file \n");
    System.out.println("--time: The time interval to execute. s:second, m: minute, h:hour  \n");
}


public static void main(String[] args) throws IOException {
    RepeatCreateSameFile command = new RepeatCreateSameFile();
    command.exec(args);
}
}