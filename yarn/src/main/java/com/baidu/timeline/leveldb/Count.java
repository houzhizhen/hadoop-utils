package com.baidu.timeline.leveldb;

import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Count extends LevelDbUtil {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        String str = args[1];
        System.out.println("dbPath: " + dbPath);
        System.out.println("str: " + str);
        AtomicLong count = new AtomicLong();
        try {
            LevelDbUtil.process(dbPath, (Map.Entry<byte[], byte[]> entry) -> {
                String key = new String(entry.getKey());
                if (key.contains(str)) {
                    count.incrementAndGet();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
        System.out.println("The number of keys that contains " + str + " is " + count);
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar com.baidu.timeline.leveldb.Count dbPath str");
    }
}
