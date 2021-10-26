package com.baidu.fs.raw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class MV implements Command{

    public static String NAME = "mv";

    @Override
    public void exec(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
        }
        URI uri = URI.create(args[0]);
        Path srcPath = new Path(uri.getPath());
        Path destPath = new Path(args[1]);
        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        try {
            fs.rename(srcPath, destPath);
        } finally {
            fs.close();
        }
    }

    public void printUsage() {
        System.out.println("mv src dest");
    }
}
