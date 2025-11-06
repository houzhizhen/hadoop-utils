package com.baidu.fs.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static com.baidu.fs.util.Constants.BYTE_ARRAY_CAPACITY;

public class FileWriter {
public static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

public static void write(FileSystem fs, Path file, long size) {
    byte[] bytesWrite = new byte[BYTE_ARRAY_CAPACITY];

    new Random().nextBytes(bytesWrite);

    long remains = size % BYTE_ARRAY_CAPACITY;
    long times = size / BYTE_ARRAY_CAPACITY;
    long beginTime = System.currentTimeMillis();
    FSDataOutputStream out = null;
    try {
        out = fs.create(file);
        beginTime = System.currentTimeMillis();
        for (long i = 0; i < times; i++) {
            out.write(bytesWrite);
        }
        if (remains > 0) {
            out.write(bytesWrite, 0, (int) remains);
        }
        out.close();
    } catch (IOException e) {
        throw new RuntimeException(e);
    } finally {
        long timeSpent = System.currentTimeMillis() - beginTime;
        LOG.info("Write {} bytes of byte array to {} using {} ms", size, file, timeSpent);
    }
}
}
