
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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelTailTest {
private static final Logger LOG = LoggerFactory.getLogger(ParallelTailTest.class);

public static void main(String[] args) throws IOException {
    new ParallelTailTest().run(args);
}

private void run(String[] args) throws IOException {
    Parameters parameters = Parameters.get(args);
    int parallel = parameters.getInt("parallel", 100);
    String path = parameters.get("path");
    int seekNum = parameters.getInt("seekNum", 100);
    int printInterval = parameters.getInt("printInterval", 100);;
    long fileSize = parameters.getLong("fileSize", 2L * 1024 * 1024 * 1024);
    long loopCount = parameters.getLong("loopCount", 1024 * 1024);
    LOG.info("parallel={}", parallel);
    LOG.info("path={}", path);
    LOG.info("seekNum={}", seekNum);
    LOG.info("fileSize={}", fileSize);
    LOG.info("loopCount={}", loopCount);
    Path rootPath = new Path(path);
    FileSystem fs = FileSystem.get(rootPath.toUri(), new Configuration());
    fs.mkdirs(rootPath);

    ExecutorService es = Executors.newFixedThreadPool(parallel);
    final AtomicBoolean stopped = new AtomicBoolean();
    final AtomicReference<Exception> eCatch = new AtomicReference<>();
    for (int threadId = 0; threadId < parallel; threadId++) {
        final int id = threadId;
        es.submit(() -> {
            Path filePath = new Path(rootPath, "file" + id);
            try {
                FSDataOutputStream out = fs.create(filePath);
                for (long i = 0; i < fileSize / Long.BYTES; i++) {
                    out.writeLong(i * 8);
                }
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Random rand = new Random();
            long loopIndex = 0;
            while(! stopped.get() && loopIndex < loopCount) {
                long pos = -1;
                if (loopIndex % printInterval == 0) {
                    LOG.info("Thread " + id + ", loopIndex:" + loopIndex);
                }
                loopIndex++;
                try (FSDataInputStream in = fs.open(filePath)) {
                    for (long i = 1; i <= seekNum; i++) {
                        pos = fileSize - i * Long.BYTES;
                        in.seek(pos);
                        long value = in.readLong();
                        if (value != pos) {
                            throw new Exception("Thread " + id + ", read pos: " + pos + " found " + value);
                        }
                    }
                    for (long i = 1; i <= seekNum; i++) {
                        pos = (Math.abs(rand.nextLong()) % (fileSize / Long.BYTES)) * Long.BYTES;
                        in.seek(pos);
                        long value = in.readLong();
                        if (value != pos) {
                            throw new Exception("Thread " + id + ", read pos: " + pos + " found " + value);
                        }
                    }
                } catch (Exception e) {
                    stopped.set(true);
                    eCatch.set(e);
                    e.printStackTrace();
                }
            }
        });

    }
}
}
