package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PutAndGetBlockLocations {

    public static final Log LOG = LogFactory.getLog(PutAndGetBlockLocations.class);
    private static final byte[] EMPTY_BYTES = new byte[1024];

    private final FileSystem fileSystem;
    private final int threadId;
    private final int subDirNum;
    private final int fileNum;
    private final Path[] subDirs;

    public PutAndGetBlockLocations(FileSystem fileSystem, Path basePath, 
                                   int threadId, int subDirNum, int fileNum) {
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

    public void listAndGetBlockLocations(int iterationTimes, AtomicBoolean stopped) throws IOException {
        int nextPrintDegree = 1;
        int i = 0;
        for (; i < iterationTimes && !stopped.get(); i++) {
            if (i == nextPrintDegree) {
                nextPrintDegree = nextPrintDegree * 10;
                LOG.info(String.format("Thread %s list %s times",
                        this.threadId, i));
            }
            Path path = this.subDirs[i % this.subDirs.length];
            FileStatus[] statuses = this.fileSystem.listStatus(path);
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
            RemoteIterator<LocatedFileStatus> itr = this.fileSystem.listLocatedStatus(path);
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
            PutAndGetBlockLocations action = new PutAndGetBlockLocations(fs, basePath, i, subDirNum, fileNum);
            es.submit(()-> {
                try {
                    action.put();
                    action.listAndGetBlockLocations(iterationTimes, stopped);
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
        LOG.info("Usage: PutAndGetBlockLocations basePath threadNum subdirNum fileNum iterationTimes");
    }
}
