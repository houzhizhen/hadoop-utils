package com.baidu.timeline.leveldb;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;

public class TTL  extends LevelDbUtil {
    private static final Log LOG = LogFactory.getLog(TTL.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        System.out.println("dbPath: " + dbPath);


    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar com.baidu.timeline.leveldb.TTL dbPath");
    }
}
