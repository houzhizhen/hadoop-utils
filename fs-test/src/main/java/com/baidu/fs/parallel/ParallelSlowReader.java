package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelSlowReader {
private static final Logger LOG = LoggerFactory.getLogger(ParallelSlowReader.class);

public static void read(FileSystem fs, List<Path> paths, int parallel,
                        int bytesPerSecond,
                        int createThreadPerSecond)  {

    ExecutorService es = Executors.newFixedThreadPool(parallel);
    final AtomicBoolean stopped = new AtomicBoolean();
    final AtomicReference<Exception> eCatch = new AtomicReference<>();
    Random random = new Random();
    for (int i = 0; i < parallel; i++) {
        es.submit(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(random.nextInt(1000));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            byte[] bytes = new byte[bytesPerSecond];
            Path path = paths.get(new Random().nextInt(paths.size()));
            try (InputStream in = fs.open(path)) {
                int len = in.read(bytes);
                while (!stopped.get() && len > 0) {
                    TimeUnit.SECONDS.sleep(1);
                    len = in.read(bytes);
                }
            } catch (IOException | InterruptedException e) {
                stopped.set(true);
                eCatch.set(e);
            }
        });
        if (i % createThreadPerSecond == 0) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

public static void main(String[] args) throws IOException {
    new ParallelSlowReader().run(args);
}

private void run(String[] args) throws IOException {
    Parameters parameters = Parameters.get(args);
    int parallel = parameters.getInt("parallel");
    String path = parameters.get("path");
    int bytesPerSecond = parameters.getInt("bytesPerSecond", 4096);
    int createThreadPerSecond = parameters.getInt("createThreadPerSecond", 1024);
    LOG.info("parallel={}", parallel);
    LOG.info("path={}", path);
    LOG.info("bytesPerSecond={}", bytesPerSecond);
    LOG.info("createThreadPerSecond={}", createThreadPerSecond);
    Path filePath = new Path(path);
    FileSystem fs = FileSystem.get(filePath.toUri(), new Configuration());
    List<Path> paths = new ArrayList<>();
    if (fs.isDirectory(filePath)) {
        FileStatus[] statuses = fs.listStatus(filePath);
        for (FileStatus status : statuses) {
            paths.add(status.getPath());
        }
    } else {
        paths.add(filePath);
    }
    LOG.info("paths:{}", paths);
    read(fs, paths, parallel, bytesPerSecond, createThreadPerSecond);
}
}
