package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Usage:
 * hadoop jar
 */
public class MakeWarehouseDirs {

    public static final Log LOG = LogFactory.getLog(MakeWarehouseDirs.class);

    private final FileSystem fileSystem;
    private int level0Index;
    private final int []countPerLevel;

    private Path level0Path;

    public MakeWarehouseDirs(FileSystem fileSystem, Path parentPath, int level0Index, int []countPerLevel) {
        this.fileSystem = fileSystem;
        this.level0Index = level0Index;
        this.countPerLevel = countPerLevel;
        this.level0Path = new Path(parentPath, "l" + level0Index);
    }

    public void run() throws IOException {
        this.fileSystem.mkdirs(level0Path);
        create(countPerLevel, 1, level0Path);
    }


    public void create(int []countPerLevel, int level, Path parentPath) throws IOException {
        if (level == countPerLevel.length - 1) {
            for (int lastIndex = 0; lastIndex < countPerLevel[level]; lastIndex++) {
                Path filePath = new Path(parentPath, "f" + lastIndex);
                FSDataOutputStream o = this.fileSystem.create(filePath);
                o.close();
            }
        } else {
            for (int index = 0; index < countPerLevel[level]; index++) {
                Path path = new Path(parentPath, "d" + index);
                this.fileSystem.mkdirs(path);
                create(countPerLevel, level + 1, path);
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
        Path basePath = new Path(uri.getPath());
        int level = args.length - 1;
        int[] countPerLevel = new int[level];
        for (int i = 0; i < countPerLevel.length; i++) {
            countPerLevel[i] = Integer.parseInt(args[i+1]);
        }

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        if (fs.exists(basePath)) {
            LOG.info("Path " + basePath + " exist, deleting");
            fs.delete(basePath, true);
        }
        fs.mkdirs(basePath);
        long beginTime = System.currentTimeMillis();
        try {

            if (countPerLevel.length <= 1) {
                MakeWarehouseDirs makeWarehouseDirs = new MakeWarehouseDirs(fs, basePath, 0, countPerLevel);
                makeWarehouseDirs.create(countPerLevel, 0, basePath);
            } else {
                ExecutorService es = Executors.newFixedThreadPool(countPerLevel[0]);
                AtomicBoolean stopped = new AtomicBoolean();
                for (int i = 0; i < countPerLevel[0]; i++) {
                    MakeWarehouseDirs makeWarehouseDirs = new MakeWarehouseDirs(fs, basePath, i, countPerLevel);
                    es.submit(() -> {
                        try {
                            makeWarehouseDirs.run();
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
        LOG.info("Usage: MakeWarehouseDirs level0DirNum, level1DirNum, ... , levelNFileNum");
    }
}