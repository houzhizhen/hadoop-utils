package com.baidu.fs.test;

import com.baidu.fs.parallel.PutAndList;
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
    public static final Log LOG = LogFactory.getLog(PutAndList.class);
    private static final byte[] EMPTY_BYTES = new byte[1024];

    private final FileSystem fileSystem;
    private final Path[] subDirs;
    private final int loopTime;
    private final int subdirCount = 60;

    public LongTimeDirTest(FileSystem fileSystem, Path basePath, int loopTime) {
        this.fileSystem = fileSystem;
        this.loopTime = loopTime;
        this.subDirs = new Path[subdirCount];
        for (int i = 0; i < subdirCount; i++) {
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
        for (int i = 0; i < this.subDirs.length; i++) {
            LOG.info("mkdir dir " + this.subDirs[i]);
            this.fileSystem.mkdirs(this.subDirs[i]);
            try {
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                // throw new RuntimeException(e);
            }
        }
        for (int i = 0; i < this.subDirs.length; i++) {
            LOG.info("delete dir " + this.subDirs[i]);
            this.fileSystem.delete(this.subDirs[i], true);
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
