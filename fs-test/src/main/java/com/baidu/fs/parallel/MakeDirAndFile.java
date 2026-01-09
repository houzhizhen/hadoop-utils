package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.Reference;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Usage:
 * hadoop jar
 */
public class MakeDirAndFile {

    public static final Logger LOG = LoggerFactory.getLogger(MakeDirAndFile.class);

    private static final int BUFFER_LENGTH = 4096;
    private final FileSystem fileSystem;
    private final int []countPerLevel;

    private Path basePath;
    private static int fileNumPerDir;
    private static byte BUFFER[] = new byte[BUFFER_LENGTH];
    private long fileLength;
    private AtomicBoolean stop;

    public MakeDirAndFile(FileSystem fileSystem, Path parentPath,
                          int index, int []countPerLevel, long fileLength,
                          AtomicBoolean stop) {
        this.fileSystem = fileSystem;
        this.countPerLevel = countPerLevel;
        this.basePath = new Path(parentPath, "d" + index);
        this.fileLength = fileLength;
        this.stop = stop;
    }

    public void run() throws IOException {
        // this.fileSystem.mkdirs(basePath);
        create(0, basePath);
    }


    public void create(int level, Path parentPath) throws IOException {
        if (level == countPerLevel.length) {
            for (int lastIndex = 0; lastIndex < fileNumPerDir && !stop.get(); lastIndex++) {
                Path filePath = new Path(parentPath, "f" + lastIndex);
                createFile(fileSystem, filePath, fileLength);
            }
        } else {
            for (int index = 0; index < countPerLevel[level]; index++) {
                Path path = new Path(parentPath, "d" + index);
                // only create dir in most deep dir
                if (level == countPerLevel.length - 1) {
                    this.fileSystem.mkdirs(path);
                }
                create(level + 1, path);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Parameters parameters = new Parameters(args);

        int dirCountPerLevelArgsIndex = 5;
        if(args.length <= dirCountPerLevelArgsIndex) {
            printUsage();
            System.exit(1);
        }

        Path basePath = new Path(parameters.get("base-path"));
        URI uri = basePath.toUri();
        int iteratorTime = parameters.getInt("iterator-time");
        int iteratorStartIndex = parameters.getInt("iterator-start-index", 0);
        int threadNum = parameters.getInt("thread-num");
        fileNumPerDir = parameters.getInt("file-per-dir");
        long fileLength = parameters.getLong("file-length");


        int[] dirCountPerLevel = parameters.getIntArray("dirs-per-level");
        int totalCreateFilesPerIterator = threadNum * fileNumPerDir;
        for (int i = 0; i < dirCountPerLevel.length; i++) {
            totalCreateFilesPerIterator *= dirCountPerLevel[i];
        }
        LOG.info("basePath={}, iteratorTime={}, iterator-start-index={}. threadNum={}, fileNumPerDir= {}," +
                "fileLength = {}, DirsPerLevel= {}, totalCreateFilesPerIterator = {}",
            basePath, iteratorTime, iteratorStartIndex, threadNum, fileNumPerDir, fileLength,
            Arrays.toString(dirCountPerLevel), totalCreateFilesPerIterator);

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        fs.mkdirs(basePath);
        long beginTime = System.currentTimeMillis();
        long createdFileNum = 0L;
        try {
            for (int i = iteratorStartIndex; i < iteratorTime; i++) {
                long iteratorTimeBegin = System.currentTimeMillis();
                Path iteratorBase = new Path(basePath, "iterator-" + i);
                runOneIterator(fs, iteratorBase, dirCountPerLevel, threadNum, fileLength);
                long iteratorTimeEnd = System.currentTimeMillis();

                LOG.info("iterator {} , created {} files from file num {} using {} ms",
                    i, totalCreateFilesPerIterator,createdFileNum, iteratorTimeEnd - iteratorTimeBegin);
                createdFileNum += totalCreateFilesPerIterator;
            }

        } finally {
            fs.close();
        }
        long timeSpend = System.currentTimeMillis() - beginTime;
        LOG.info("Total time for all iterator used: " + timeSpend +" ms");
    }

    private static void runOneIterator(FileSystem fs, Path iteratorBase,
                                int[] dirCountPerLevel, int threadNum, long fileLength) throws IOException {
        AtomicBoolean stop = new AtomicBoolean();
        if (threadNum == 1) {
            MakeDirAndFile makeWarehouseDirs = new MakeDirAndFile(fs, iteratorBase, 0, dirCountPerLevel,fileLength, stop);
            makeWarehouseDirs.run();
        } else {
            ExecutorService es = Executors.newFixedThreadPool(threadNum);

            AtomicReference<Exception> eRef = new AtomicReference<>();
            for (int i = 0; i < threadNum; i++) {
                MakeDirAndFile makeDirAndFile = new MakeDirAndFile(fs, iteratorBase, i, dirCountPerLevel, fileLength, stop);
                es.submit(() -> {
                    try {
                        makeDirAndFile.run();
                    } catch (IOException e) {
                        stop.set(true);
                        eRef.set(e);
                        e.printStackTrace();
                    }
                });
            }
            es.shutdown();
            try {
                es.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (eRef.get() !=null) {
                throw new RuntimeException(eRef.get());
            }
        }

    }

    private static void printUsage() {
        LOG.info("Usage: MakeDirAndFiles  baseDir iteratorTime threadNum fileNumPerDir fileLength level0DirNum, level1DirNum, ... , levelNFileNum");
    }

private void createFile(FileSystem fs, Path path, long size)  {

    long beginTime = System.currentTimeMillis();
    FSDataOutputStream out = null;
    long times = size / BUFFER_LENGTH;
    long remains = size % BUFFER_LENGTH;
    try {
        out = fs.create(path);
        for (long i = 0; i < times; i++) {
            out.write(BUFFER);
        }
        if (remains > 0) {
            out.write(BUFFER, 0, (int) remains);
        }
        out.close();
    } catch (IOException e) {
        stop.set(true);
        throw new RuntimeException(e);
    } finally {
        long timeSpent = System.currentTimeMillis() - beginTime;
        LOG.info("Write {} bytes of byte array using {} ms", size, timeSpent);
    }
}
}