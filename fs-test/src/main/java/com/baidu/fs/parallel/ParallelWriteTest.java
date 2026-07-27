package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
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

public class ParallelWriteTest extends Thread {
private static final Logger LOG = LoggerFactory.getLogger(ParallelWriteTest.class);

private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

private final int parallel;
private final Path basePath;
private final long filesize;
private final int bufferSize;
private final int fileNumPerThread;
private final boolean deleteAfterWrite;
private final byte[] bytes;
private FileSystem fs;
private CountDownLatch latch;
private final AtomicReference<Throwable> failure = new AtomicReference<>();

public ParallelWriteTest(Parameters parameters, CountDownLatch latch) {
    this.parallel = parameters.getInt("parallel");
    this.basePath = new Path(parameters.get("basePath"));
    this.filesize = parameters.getBytes("filesize", 4096L);
    long configuredBuffer = parameters.getBytes("bufferSize", DEFAULT_BUFFER_SIZE);
    // buffer 不必大于 filesize，避免小文件场景多分配内存
    long effectiveBuffer = Math.min(configuredBuffer, filesize);
    if (effectiveBuffer > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("bufferSize too large: " + effectiveBuffer
            + ", must be <= " + Integer.MAX_VALUE);
    }
    this.bufferSize = (int) effectiveBuffer;
    this.bytes = new byte[bufferSize];
    this.fileNumPerThread = parameters.getInt("fileNumPerThread", 100);
    this.deleteAfterWrite = parameters.getBoolean("deleteAfterWrite", false);
    LOG.info("parallel={}", parallel);
    LOG.info("basePath={}", basePath);
    LOG.info("filesize={}", filesize);
    LOG.info("bufferSize={}", bufferSize);
    LOG.info("fileNumPerThread={}", fileNumPerThread);
    LOG.info("deleteAfterWrite={}", deleteAfterWrite);
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
    ParallelWriteTest test = new ParallelWriteTest(parameters, latch);
    test.start();
    latch.await(1, TimeUnit.HOURS);
    test.checkFailure();
    System.out.println("ParallelWriteTest finished");
}

public void run() {
    runInterval();
}

/**
 * 若有任一 worker 线程抛异常，则将首个异常包装为 IOException 抛出。
 * 供上层（如 {@link ParallelReadWriteByPercent}）在 {@link CountDownLatch#await} 后调用，
 * 以避免"任务已挂但流程看起来成功"的情况。
 */
public void checkFailure() throws IOException {
    Throwable t = failure.get();
    if (t != null) {
        throw new IOException("ParallelWriteTest failed: " + t, t);
    }
}

public void runInterval() {
    ExecutorService es = Executors.newFixedThreadPool(parallel);
    final AtomicBoolean stopped = new AtomicBoolean();
    try {
        for (int i = 0; i < parallel; i++) {
            final int threadNum = i;
            es.submit(() -> {
                Path path = new Path(basePath, "sub" + threadNum);
                try {
                    fs.mkdirs(path);
                    for (int fileNum = 0; fileNum < fileNumPerThread && ! stopped.get(); fileNum++) {
                        Path filePath = new Path(path, "file-" + fileNum);
                        FSDataOutputStream out = fs.create(filePath);
                        try {
                            long remaining = filesize;
                            while (remaining > 0 && !stopped.get()) {
                                int toWrite = (int) Math.min((long) bufferSize, remaining);
                                out.write(bytes, 0, toWrite);
                                remaining -= toWrite;
                            }
                        } finally {
                            out.close();
                        }
                        if (deleteAfterWrite) {
                            fs.delete(filePath);
                        }
                    }
                } catch (Throwable t) {
                    stopped.set(true);
                    failure.compareAndSet(null, t);
                    LOG.warn("thread {} failed", threadNum, t);
                } finally {
                    // LOG.info("thread " + threadNum + " finished");
                    latch.countDown();
                }
            });

        }
    } finally {
        es.shutdown();
    }
}

}
