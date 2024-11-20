package com.baidu.fs.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * 长时间 Dir 测试，每轮创建100个目录，然后删除这100个目录。
 * 每个目录操作 sleep 1 分钟，那么创建 100 个目录大约 sleep 100 分钟。
 */
public class LongTimeDirTest {
    public static final Log LOG = LogFactory.getLog(LongTimeDirTest.class);

    private final FileSystem fileSystem;
    private final Path[] subDirs;
    private final int loopTime;
    private static final int SUB_DIR_COUNT = 60;

    public LongTimeDirTest(FileSystem fileSystem, Path basePath, int loopTime) {
        this.fileSystem = fileSystem;
        this.loopTime = loopTime;
        this.subDirs = new Path[SUB_DIR_COUNT];
        for (int i = 0; i < SUB_DIR_COUNT; i++) {
            this.subDirs[i] = new Path(basePath, "subdir_" + i);
        }
    }

    public void run() throws IOException {
        for (int i = 0; i < loopTime; i++){
            LOG.info("loopTime " + loopTime);
            executeOneLoop();
        }

    }
    private void executeOneLoop() throws IOException {
        for (Path subDir : this.subDirs) {
            LOG.info("mkdir dir " + subDir);
            this.fileSystem.mkdirs(subDir);
            try {
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                // throw new RuntimeException(e);
            }
        }
        for (Path subDir : this.subDirs) {
            LOG.info("delete dir " + subDir);
            this.fileSystem.delete(subDir, true);
            try {
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                // throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }

        LOG.info(String.format("basePath='%s', loopTime=%s", args[0], args[1]));

        URI uri = URI.create(args[0]);
        Path basePath = new Path(uri.getPath());
        int loopTime = Integer.parseInt(args[1]);

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        fs.mkdirs(basePath);

        LongTimeDirTest makeDirs = new LongTimeDirTest(fs, basePath, loopTime);
        makeDirs.run();
    }

    private static void printUsage() {
        LOG.info("Usage: LongTimeDirTest basePath loopTime");
    }
}
