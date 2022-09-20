package com.baidu.timeline.leveldb;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public class LevelDbUtil {

    public static void process(String dbPath, Consumer<Map.Entry<byte[], byte[]>> action) throws IOException {
        JniDBFactory factory = JniDBFactory.factory;
        DB db = factory.open(new File(dbPath), new Options());
        try {
            DBIterator iterator = db.iterator();
            iterator.seekToFirst();
            while(iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                action.accept(entry);
            }
            iterator.close();
        } finally {
            db.close();
        }
    };
}
