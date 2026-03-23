package com.baidu.fs.raw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class Read  implements Command {
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
        InputStream in = fs.open(srcPath);
        OutputStream out = LocalFileSystem.get(conf).create(destPath);
        IOUtils.copyBytes(in, out, 4096);
        in.close();
        out.close();
    } finally {
        fs.close();
    }
}

public void printUsage() {
    System.out.println("append dest local-file");
}

public static void main(String[] args) throws IOException {
    Read read = new Read();
    read.exec(new String[] {"hdfs://localhost:8020/host1", "./hosts"});
}
}
