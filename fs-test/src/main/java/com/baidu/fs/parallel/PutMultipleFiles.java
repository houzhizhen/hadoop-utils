package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PutMultipleFiles {

    public static final Log LOG = LogFactory.getLog(PutAndList.class);
    private static final byte[] EMPTY_BYTES = new byte[1024];

    private FileSystem fileSystem;
    private Path basePath;
    private int fileSize;
    private FSDataOutputStream[] outputs;

    public PutMultipleFiles(FileSystem fileSystem, Path basePath, int fileNum, int fileSize) {
        this.fileSystem = fileSystem;
        this.basePath = basePath;

        this.fileSize = fileSize;
        this.outputs = new FSDataOutputStream[fileNum];
    }

    public void write() throws IOException {
        int length = fileSize;
        while (length > 0) {
            int batch = Math.min(length, EMPTY_BYTES.length);
            write(batch);
            length -= batch;
        }
    }

    private void write(int size) throws IOException {
        for (int i = 0; i < outputs.length; i++) {
            outputs[i].write(EMPTY_BYTES, 0, size);
        }
    }

    private void open() throws IOException {
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = fileSystem.create(new Path(basePath, "file_"+i));
        }
    }
    
    private void close() throws IOException {
        for (int i = 0; i < outputs.length && outputs[i] != null; i++) {
            outputs[i].close();
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 3) {
            printUsage();
            System.exit(1);
        }
        URI uri = URI.create(args[0]);
        Path basePath = new Path(uri.getPath());
        int fileNum = Integer.parseInt(args[1]);
        int fileSize = Integer.parseInt(args[2]);

        LOG.info(String.format("basePath='%s', fileNum=%s, fileSize=%s",
                               basePath, fileNum, fileSize));

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        fs.mkdirs(basePath);
        PutMultipleFiles putMultipleFiles = new PutMultipleFiles(fs, basePath, fileNum, fileSize);
        try {
            putMultipleFiles.open();
            putMultipleFiles.write();
        } finally {
            putMultipleFiles.close();
            fs.close();
        }
    }

    private static void printUsage() {
        LOG.info("Usage: PutMultipleFiles basePath fileNum fileSize");
    }
}
