package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
 * Parallel put and list.
 * Each thread create specified number of subdirectories,
 * and put specified number of file in each subdirectory.
 * Then each file list the file in each subdirectory, list specified times.
 *
 * The directories in ${base_directory} look like:
 * ${base_directory}/thread_0
 * ${base_directory}/thread_1
 * ...
 * ${base_directory}/thread_n
 *
 * The directories in ${base_directory}/thread_${i} look like:
 * ${base_directory}/thread_${i}/subdir_0
 * ${base_directory}/thread_${i}/subdir_1
 * ...
 * ${base_directory}/thread_${i}/subdir_m
 * (m means subdirectories created by each thread)
 *
 * The files in ${base_directory}/thread_${i}/subdir_${j} look like:
 * ${base_directory}/thread_${i}/subdir_${j}/file_0
 * ${base_directory}/thread_${i}/subdir_${j}/file_1
 * ...
 * ${base_directory}/thread_${i}/subdir_${j}/file_k
 * (k means files created in each subdirectory)
 *
 * Then each thread list the subdirectories one by one, list specified times.
 */
public class PutAndList {

    public static final Log LOG = LogFactory.getLog(PutAndList.class);
    private static final byte[] EMPTY_BYTES = new byte[1024];

    private final FileSystem fileSystem;
    private final int threadId;
    private final int subDirNum;
    private final int fileNum;
    private final Path[] subDirs;

    public PutAndList(FileSystem fileSystem, Path basePath, int threadId, int subDirNum,
                      int fileNum) {
        this.fileSystem = fileSystem;
        this.threadId = threadId;
        this.subDirNum = subDirNum;
        this.fileNum = fileNum;
        Path threadPath = new Path(basePath, "thread_" + threadId);
        this.subDirs = new Path[subDirNum];
        for (int i = 0; i < subDirNum; i++) {
            this.subDirs[i] = new Path(threadPath, "subdir_" + i);
        }
    }

    public void put() throws IOException {
        for (int i = 0; i < this.subDirs.length; i++) {
            this.fileSystem.mkdirs(this.subDirs[i]);
        }
        for (int i = 0; i < this.subDirs.length; i++) {
            for (int j = 0; j < this.fileNum; j++) {
                Path filePath = new Path(this.subDirs[i], "file_" + j);
                FSDataOutputStream out = this.fileSystem.create(filePath);
                out.write(EMPTY_BYTES);
                out.close();
            }
        }
    }

    public void list(int iterationTimes, AtomicBoolean stopped) throws IOException {
        int nextPrintDegree = 1;
        int i = 0
        for (; i < iterationTimes && !stopped.get(); i++) {
            if (i == nextPrintDegree) {
                nextPrintDegree = nextPrintDegree * 10;
                LOG.info(String.format("Thread %s list %s times",
                                       this.threadId, i));
            }
            FileStatus[] statuses = this.fileSystem.listStatus(this.subDirs[i % this.subDirs.length]);
            if (statuses.length != this.fileNum) {
                stopped.set(true);
                synchronized (com.baidu.fs.parallel.List.class) {
                    LOG.error(String.format("Thread %s get %s files",
                                            this.threadId, statuses.length));
                    for (FileStatus status : statuses) {
                        LOG.info(status.getPath());
                    }
                }
            }
        }
        LOG.info(String.format("Thread %s list %s times, over.",
                              this.threadId, i));
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 5) {
            printUsage();
            System.exit(1);
        }

        LOG.info(String.format("basePath='%s', threadNum=%s, subdirNum=%s, fileNum=%s, iterationTimes=%s",
                               args[0], args[1], args[2], args[3], args[4]));

        URI uri = URI.create(args[0]);
        Path basePath = new Path(uri.getPath());
        int threadNum = Integer.parseInt(args[1]);
        int subDirNum = Integer.parseInt(args[2]);
        int fileNum = Integer.parseInt(args[3]);
        int iterationTimes = Integer.parseInt(args[4]);

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        fs.mkdirs(basePath);

        ExecutorService es = Executors.newFixedThreadPool(threadNum);
        AtomicBoolean stopped = new AtomicBoolean();
        for (int i = 0; i < threadNum; i++) {
            PutAndList putAndList = new PutAndList(fs, basePath, i, subDirNum, fileNum);
            es.submit(()-> {
                try {
                    putAndList.put();
                    putAndList.list(iterationTimes, stopped);
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
        } finally {
            fs.close();
        }
    }

    private static void printUsage() {
        LOG.info("Usage: PutAndList basePath threadNum subdirNum fileNum iterationTimes");
    }
}
