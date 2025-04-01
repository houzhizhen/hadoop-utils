package com.baidu.fs.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeleteWarehouseDirs {

    public static final Log LOG = LogFactory.getLog(DeleteWarehouseDirs.class);

    private final FileSystem fileSystem;

    private Path path;

    public DeleteWarehouseDirs(FileSystem fileSystem, Path path) {
        this.fileSystem = fileSystem;
        this.path = path;
    }

    public void run() throws IOException {
        this.fileSystem.delete(path, true);
    }


    public static void main(String[] args) throws IOException {
        if(args.length < 1) {
            printUsage();
            System.exit(1);
        }

        LOG.info(String.format("basePath='%s'",
                args[0]));

        URI uri = URI.create(args[0]);
        Path basePath = new Path(uri.getPath());

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        if (!fs.exists(basePath)) {
            LOG.info("Path " + basePath + "  does not exist");
            fs.close();
            return;
        }
        FileStatus[] statuses = fs.listStatus(basePath);

        try {

            ExecutorService es = Executors.newFixedThreadPool(statuses.length);
            for (FileStatus status : statuses) {
                DeleteWarehouseDirs deleteWarehouseDirs = new DeleteWarehouseDirs(fs, status.getPath());
                es.submit(() -> {
                    try {
                        deleteWarehouseDirs.run();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            es.shutdown();
            try {
                es.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } finally {
            fs.close();
        }
    }

    private static void printUsage() {
        LOG.info("Usage: DeleteWarehouseDirs rootPath");
    }
}
