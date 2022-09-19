package com.baidu.timeline.leveldb;

import org.iq80.leveldb.DBIterator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Dump extends LevelDbBase {

    private String dumpFile;

    protected Dump(String dbPath, String dumpFile) {
        super(dbPath);
        this.dumpFile = dumpFile;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        String dumpFile = args[1];
        System.out.println("dbPath: " + dbPath);
        System.out.println("dumpFile: " + dumpFile);

        Dump dump = new Dump(dbPath, dumpFile);

        try {
            dump.open();
            dump.process();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            dump.close();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar com.baidu.timeline.leveldb.Dump dbPath dumpFile");
    }

    @Override
    public void process() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(dumpFile));
        try {
            DBIterator iterator = this.db.iterator();
            try {
                for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                    String key = new String(iterator.peekNext().getKey());
                    String value = new String((iterator.peekNext().getValue()));
                    writer.write(key+" = "+value +"\n");
                }
            } finally {
                // Make sure you close the iterator to avoid resource leaks.
                iterator.close();
            }

        } finally {
            db.close();
            writer.close();
        }
    }
}
