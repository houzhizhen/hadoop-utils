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
 * Parallel repeat put.
 * Each thread create specified number of subdirectories,
 * and put specified number of file in each subdirectory.
 * Then each thread put the file to each subdirectory with specified times.
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
 */
public class RepeatPut {

    public static final Log LOG = LogFactory.getLog(PutAndList.class);
    private static final byte[] EMPTY_BYTES = new byte[1024];

    private final FileSystem fileSystem;
    private final int threadId;
    private final int subDirNum;
    private final int fileNum;
    private final Path[] subDirs;

    public RepeatPut(FileSystem fileSystem, Path basePath, int threadId, int subDirNum,
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

    public void put(int iterationTimes, AtomicBoolean stopped) {
        try {
            this.createSubDirs();
            for (int i = 0; i < iterationTimes && !stopped.get(); i++) {
                this.putFiles();
                LOG.info(String.format("Thread %s put %s times",
                                       this.threadId, i));
            }
        } catch (IOException e) {
            e.printStackTrace();
            stopped.set(true);
        }
    }

    private void createSubDirs() throws IOException {
        for (int i = 0; i < this.subDirs.length; i++) {
            this.fileSystem.mkdirs(this.subDirs[i]);
        }
    }

    public void putFiles() throws IOException {
        for (int i = 0; i < this.subDirs.length; i++) {
            for (int j = 0; j < this.fileNum; j++) {
                Path filePath = new Path(this.subDirs[i], "file_" + j);
                FSDataOutputStream out = this.fileSystem.create(filePath);
                out.write(EMPTY_BYTES);
                out.close();
            }
        }
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
            RepeatPut repeatPut = new RepeatPut(fs, basePath, i, subDirNum, fileNum);
            es.submit(()-> {
               repeatPut.put(iterationTimes, stopped);
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