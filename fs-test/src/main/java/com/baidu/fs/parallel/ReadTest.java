package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReadTest {
public static final Log LOG = LogFactory.getLog(ReadTest.class);

private final Configuration conf;
private FileSystem fileSystem;
private final int []countPerLevel;

private Path level0Path;
private boolean useSeparateConnPerThread;

public ReadTest(Configuration conf, Path parentPath, int level0Index, int []countPerLevel) {
    this.conf = conf;
    this.useSeparateConnPerThread = conf.getBoolean("dfs.read-test.use.seperate.conn", false);
    this.countPerLevel = countPerLevel;
    this.level0Path = new Path(parentPath, "l" + level0Index);
}

public void run() throws IOException {
    try {
        if (this.useSeparateConnPerThread) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser("testuser" + level0Path.getName());
            fileSystem = ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(level0Path.toUri(), conf));
        } else {
            fileSystem = FileSystem.get(level0Path.toUri(), conf);
        }

        read(countPerLevel, 1, level0Path);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    } finally {
        if (this.useSeparateConnPerThread) {
            fileSystem.close();
        }
    }

}


public void read(int []countPerLevel, int level, Path parentPath) throws IOException {
    if (level == countPerLevel.length - 1) {
        for (int lastIndex = 0; lastIndex < countPerLevel[level]; lastIndex++) {
            Path filePath = new Path(parentPath, "d" + lastIndex);
            this.fileSystem.exists(filePath);
        }
    } else {
        for (int index = 0; index < countPerLevel[level]; index++) {
            Path path = new Path(parentPath, "d" + index);
            this.fileSystem.exists(path);
            read(countPerLevel, level + 1, path);
        }
    }

}

public static void main(String[] args) throws IOException {
    if(args.length < 1) {
        printUsage();
        System.exit(1);
    }

    LOG.info(String.format("basePath='%s', level0DirNum, level1DirNum, ... , levelNFileNum",
        args[0]));

    URI uri = URI.create(args[0]);
    Path basePath = new Path(args[0]);
    int level = args.length - 1;
    int[] countPerLevel = new int[level];
    for (int i = 0; i < countPerLevel.length; i++) {
        countPerLevel[i] = Integer.parseInt(args[i+1]);
    }

    Configuration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);
    long beginTime = System.currentTimeMillis();
    try {

        if (countPerLevel.length <= 1) {
            ReadTest readTest = new ReadTest(conf, basePath, 0, countPerLevel);
            readTest.run();
        } else {
            ExecutorService es = Executors.newFixedThreadPool(countPerLevel[0]);
            AtomicBoolean stopped = new AtomicBoolean();
            for (int i = 0; i < countPerLevel[0]; i++) {

                ReadTest readTest = new ReadTest(conf, basePath, i, countPerLevel);
                es.submit(() -> {
                    try {
                        readTest.run();
                    } catch (IOException e) {
                        stopped.set(true);
                        e.printStackTrace();
                    }
                });
            }
            es.shutdown();
            try {
                es.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    } finally {
        fs.close();
    }
    long timeSpend = System.currentTimeMillis() - beginTime;
    LOG.info("Time used: " + timeSpend +" ms");
}

private static void printUsage() {
    LOG.info("Usage: ReadTest level0DirNum, level1DirNum, ... , levelNFileNum");
}
}
