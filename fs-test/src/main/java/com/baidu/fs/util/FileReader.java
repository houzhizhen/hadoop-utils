package com.baidu.fs.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.baidu.fs.util.Constants.BYTE_ARRAY_CAPACITY;

public class FileReader {
public static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

public static void readFile(FileSystem fs, Path file, long expectedLength) throws IOException {
    FileStatus status = fs.getFileStatus(file);
    long size = status.getLen();
    if (expectedLength != size) {
        throw new RuntimeException("file.length " + status.getLen() + " not equals expectedLength" + expectedLength);
    }
    byte[] bytes = new byte[BYTE_ARRAY_CAPACITY];
    long remains = size % BYTE_ARRAY_CAPACITY;
    long times = size / BYTE_ARRAY_CAPACITY;
    long beginTime = System.currentTimeMillis();
    FSDataInputStream in = null;
    try {
        in = fs.open(file);
        for (long i = 0; i < times; i++) {
            in.readFully(bytes);
        }
        if (remains > 0) {
            byte[] remainsBytes = new byte[(int) remains];
            in.readFully(remainsBytes);
        }
    } catch (IOException e) {
        throw new RuntimeException(e);
    } finally {
        if (in != null) {
            in.close();
        }
        long timeSpent = System.currentTimeMillis() - beginTime;
        LOG.info("Read {} bytes from file {} using {} ms", size, file, timeSpent);
    }
}
}
