package com.baidu.fs.parallel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ListTest {

    public static void main(String[] args) throws IOException {
        if(args.length != 4) {
            printUsage();
            System.exit(1);
        }
        int threadNum = Integer.parseInt(args[0]);
        URI uri = URI.create(args[1]);
        Path path = new Path(uri.getPath());
        int iterationTimes = Integer.parseInt(args[2]);
        int expectedCount = Integer.parseInt(args[3]);
        System.out.println(String.format("threadNum=%s, path='%s', iterationTimes=%s, expectedCount=%s",
                                         threadNum, args[1], iterationTimes, expectedCount));
        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        ExecutorService es = Executors.newFixedThreadPool(threadNum);
        AtomicBoolean stopped = new AtomicBoolean();
        for (int i = 0; i < threadNum; i++) {
            es.submit(()-> {
                try {
                    for (int j = 0; j < iterationTimes && !stopped.get(); j++) {
                        FileStatus[] statuses = fs.listStatus(path);
                        if (statuses.length != expectedCount) {
                            synchronized (ListTest.class) {
                                System.err.println(String.format("Thread %s get %s files",
                                                                 Thread.currentThread().getId(),
                                                                 statuses.length));
                                for (FileStatus status : statuses) {
                                    System.err.println(status.getPath());
                                }
                            }
                            stopped.set(true);
                        }
                    }
                } catch (IOException e) {
                    stopped.set(true);
                    e.printStackTrace();
                }
            });
        }
        es.shutdown();
        fs.close();
    }

    private static void printUsage() {
        System.out.println("Usage: List threadNum location iterationTimes expectedCount");
    }
}
