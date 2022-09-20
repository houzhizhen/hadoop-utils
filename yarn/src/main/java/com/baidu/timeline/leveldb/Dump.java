package com.baidu.timeline.leveldb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class Dump extends LevelDbUtil {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        String dumpFile = args[1];
        System.out.println("dbPath: " + dbPath);
        System.out.println("dumpFile: " + dumpFile);

        BufferedWriter writer = new BufferedWriter(new FileWriter(dumpFile));
        try {
            LevelDbUtil.process(dbPath, (Map.Entry<byte[], byte[]> entry) -> {
                String key = new String(entry.getKey());
                String value = new String(entry.getValue());
                try {
                    writer.write(key+" = "+value +"\n");
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("");
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writer.close();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar com.baidu.timeline.leveldb.Dump dbPath dumpFile");
    }
}
