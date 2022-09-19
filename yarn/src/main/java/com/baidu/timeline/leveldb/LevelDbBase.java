package com.baidu.timeline.leveldb;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;

public abstract class LevelDbBase {

    private final String dbPath;
    protected DB db;

    protected LevelDbBase(String dbPath) {
        this.dbPath = dbPath;
    }

    public void open() throws IOException {
        JniDBFactory factory = JniDBFactory.factory;
        this.db = factory.open(new File(dbPath.toString()), new Options());
    }

    public abstract void process() throws IOException;

    public void close() throws IOException {
        if (this.db != null) {
            this.db.close();
        }
    }
}
