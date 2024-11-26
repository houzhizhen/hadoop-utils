package com.baidu.fs.raw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class Append implements Command {
    public static String NAME = "append";

    @Override
    public void exec(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
        }
        File file = new File(args[1]);
        if (! file.exists()) {
            throw new FileNotFoundException(file.getPath());
        }
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        URI uri = URI.create(args[0]);
        Path srcPath = new Path(uri.getPath());
        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        try {
            OutputStream out = fs.append(srcPath);
            IOUtils.copyBytes(in, out, conf, true);
        } finally {
            fs.close();
        }
    }

    public void printUsage() {
        System.out.println("append dest local-file");
    }

    public static void main(String[] args) throws IOException {
        Append append = new Append();
        append.exec(new String[] {"/tmp/a", "/etc/profile"});
    }
}
