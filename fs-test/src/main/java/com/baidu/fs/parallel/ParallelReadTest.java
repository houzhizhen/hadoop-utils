package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelReadTest extends Thread {
private static final Logger LOG = LoggerFactory.getLogger(ParallelReadTest.class);

private final int parallel;
private final Path basePath;
private final int filesize;
private final int fileNumPerThread;
private final byte[] bytes;
private FileSystem fs;
private CountDownLatch latch;


public ParallelReadTest(Parameters parameters, CountDownLatch latch) {
    this.parallel = parameters.getInt("parallel");
    this.basePath = new Path(parameters.get("basePath"));
    this.filesize = parameters.getInt("filesize", 4096);
    this.bytes = new byte[filesize];
    this.fileNumPerThread = parameters.getInt("fileNumPerThread", 100);
    LOG.info("parallel={}", parallel);
    LOG.info("basePath={}", basePath);
    LOG.info("filesize={}", filesize);
    LOG.info("fileNumPerThread={}", fileNumPerThread);
    try {
        this.fs = FileSystem.get(basePath.toUri(), new Configuration());
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
    this.latch = latch;

}

public static void main(String[] args) throws IOException, InterruptedException {
    Parameters parameters = Parameters.get(args);
    int parallel = parameters.getInt("parallel");
    CountDownLatch latch = new CountDownLatch(parallel);
    new ParallelReadTest(parameters, latch).start();
    latch.await(1, TimeUnit.HOURS);
    System.out.println("ParallelReadTest finished");
}

public void run() {
    runInterval();
}

public void runInterval() {
    ExecutorService es = Executors.newFixedThreadPool(parallel);
    final AtomicBoolean stopped = new AtomicBoolean();
    final AtomicReference<Exception> eCatch = new AtomicReference<>();
    for (int i = 0; i < parallel; i++) {
        final int threadNum = i;
        es.submit(() -> {
            Path filePath = new Path(basePath, "file-" + threadNum);
            try {
                FSDataOutputStream out = fs.create(filePath);
                out.write(bytes);
                out.close();
                byte[] tmpBytes = new byte[bytes.length];
                for (int fileNum = 0; fileNum < fileNumPerThread && ! stopped.get(); fileNum++) {
                    FSDataInputStream in = fs.open(filePath);
                    in.read(tmpBytes);
                    in.close();
                }
            } catch (IOException e) {
                stopped.set(true);
                eCatch.set(e);
            } finally {
               // LOG.info("thread " + threadNum + " finished");
                latch.countDown();
            }
        });
    }
    es.shutdown();
}

}
