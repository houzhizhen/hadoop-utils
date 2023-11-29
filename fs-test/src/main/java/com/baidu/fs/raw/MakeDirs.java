package com.baidu.fs.raw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class MakeDirs implements Command {
    public static String NAME = "mkdirs";

    @Override
    public void exec(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
        }
        URI uri = URI.create(args[0]);
        Path srcPath = new Path(uri.getPath());
        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        try {
            boolean result = fs.mkdirs(srcPath);
            System.out.println("mkdirs " + srcPath + ": " + result);
        } finally {
            fs.close();
        }
    }

    public void printUsage() {
        System.out.println("mkdirs dest");
    }
}
