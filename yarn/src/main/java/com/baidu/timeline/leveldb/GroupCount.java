package com.baidu.timeline.leveldb;

import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Group count each category.
 */
public class GroupCount  extends LevelDbUtil {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        System.out.println("dbPath: " + dbPath);
        Map<Byte, AtomicLong> map = new HashMap<>();
        LevelDbUtil.process(dbPath, (Map.Entry<byte[], byte[]> entry) -> {
            byte[] key = entry.getKey();
            byte prefix = ' ';
            if (key.length > 0) {
                prefix = entry.getKey()[0];
            }
            AtomicLong count = map.get(prefix);
            if (count == null) {
                count = new AtomicLong(1);
                map.put(prefix, count);
            }
            count.incrementAndGet();
        });

        for (Map.Entry<Byte, AtomicLong> entry : map.entrySet()) {
            byte[] array = new byte[] {entry.getKey()};
            System.out.println("type:" + new String(array) + ", count: " + entry.getValue().get());
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar com.baidu.timeline.leveldb.GroupCount dbPath");
    }
}
