package com.baidu.fs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class CreateSubDirsInDirectory {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            printUsage();
        }
        URI uri = URI.create(args[0]);
        int times = Integer.parseInt(args[1]);
        int printInterval = 10000;
        if (args.length > 2) {
            printInterval = Integer.parseInt(args[2]);
        }
        Path srcPath = new Path(uri.getPath());
        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        try {
            for (int i = 0; i < times; i+= printInterval) {
                long fromTime = System.currentTimeMillis();
                int length = printInterval;
                if (printInterval + i > times) {
                    length = (times - i);
                }
                mksubDirs(fs, srcPath, i, i + length);
                long endTime = System.currentTimeMillis();
                System.out.println("Create " + length + " dirs at " +
                        i + " using " + (endTime - fromTime) + "ms");
            }
        } finally {
            fs.close();
        }
    }
    private static void mksubDirs(FileSystem fs, Path parent, int fromIndex, int endIndex) throws IOException {
        for (;fromIndex < endIndex; fromIndex++) {
            fs.mkdirs(new Path(parent, "subdir-" + fromIndex));
        }
    }

    private static void printUsage() {
        System.out.println("There are must be at least two parameters");
        System.exit(1);
    }
}
