package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class WriteReadTest {
public static final Log LOG = LogFactory.getLog(WriteReadTest.class);

public static void main(String[] args) throws IOException {
    if (args.length < 1) {
        printUsage();
        System.exit(1);
    }

    LOG.info(String.format("basePath='%s', level0DirNum, level1DirNum, ... , levelNFileNum",
        args[0]));

    URI uri = URI.create(args[0]);
    Path basePath = new Path(uri.getPath());
    int level = args.length - 1;
    int[] countPerLevel = new int[level];
    for (int i = 0; i < countPerLevel.length; i++) {
        countPerLevel[i] = Integer.parseInt(args[i + 1]);
    }

    Configuration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);
    if (fs.exists(basePath)) {
        LOG.info("Path " + basePath + " exist, deleting");
        fs.delete(basePath, true);
    }
    fs.mkdirs(basePath);
    WriteTest.main(args);
    ReadTest.main(args);
}

private static void printUsage() {
    LOG.info("Usage: WriteReadTest level0DirNum, level1DirNum, ... , levelNFileNum");
}
}
