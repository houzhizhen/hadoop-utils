package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PutAndListAndInterrupt {
    public static final Log LOG = LogFactory.getLog(PutAndListAndInterrupt.class);

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
        java.util.List<PutAndList> list = new ArrayList(threadNum);
        for (int i = 0; i < threadNum; i++) {
            PutAndList putAndList = new PutAndList(fs, basePath, i, subDirNum, fileNum);
            list.add(putAndList);
            putAndList.put();
        }
        LOG.info("put finished");
        list.iterator().forEachRemaining(putAndList -> {
            es.submit(()-> {
                try {
                    putAndList.list(iterationTimes, stopped);
                } catch (IOException e) {
                    stopped.set(true);
                    e.printStackTrace();
                }
            });
        });

        es.shutdownNow();
        try {
            es.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }
    private static void printUsage() {
        LOG.info("Usage: PutAndListAndInterrupt basePath threadNum subdirNum fileNum iterationTimes");
    }
}
