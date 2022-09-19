package com.baidu.timeline.leveldb;

import org.iq80.leveldb.DBIterator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Count extends LevelDbBase {
    private String str;

    protected Count(String dbPath, String str) {
        super(dbPath);
        this.str = str;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        String str = args[1];
        System.out.println("dbPath: " + dbPath);
        System.out.println("str: " + str);
        Count count = new Count(dbPath, str);

        try {
            count.open();
            count.process();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            count.close();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar com.baidu.timeline.leveldb.Count dbPath str");
    }

    @Override
    public void process() throws IOException {
        long count = 0;
        try {
            DBIterator iterator = this.db.iterator();
            try {
                for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                    String key = new String(iterator.peekNext().getKey());
                    if (key.contains(str)) {
                        count++;
                    }
                }
            } finally {
                // Make sure you close the iterator to avoid resource leaks.
                iterator.close();
            }

        } finally {
            db.close();
        }
        System.out.println("The number of keys that contains " + str + " is " + count);
    }
}
