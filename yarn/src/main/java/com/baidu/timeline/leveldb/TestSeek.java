package com.baidu.timeline.leveldb;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;

public class TestSeek {

    public static void main(String[] args) throws IOException {
        String dbPath = "/tmp/db-path";
        JniDBFactory factory = JniDBFactory.factory;
        Options options = new Options();
        options.createIfMissing(true);
        DB db = factory.open(new File(dbPath), options);
        long startTime = 1495777335060L;
        byte[] value = new byte[1024];
        for (long time = startTime; time < startTime + 100; time++) {
            byte[] reverseTimestamp = writeReverseOrderedLong(time);
            db.put(reverseTimestamp, value);
        }
        DBIterator it = db.iterator();
        long seekTime = 1495777335060L + 10L;
        it.seek(writeReverseOrderedLong(seekTime));
        while(it.hasNext()) {
            Map.Entry<byte[], byte[]> entry = it.next();
            long time = readReverseOrderedLong(entry.getKey(), 0);
            System.out.println("read " + time);
        }
        it.close();
        db.close();
    }
}
