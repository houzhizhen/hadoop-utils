package com.baidu.fs.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BosConflictTest {
    public static final Log LOG = LogFactory.getLog(BosConflictTest.class);

    private final FileSystem mainFs;
    private final FileSystem stateStoreFs;
    private final FileSystem deleteFS;
    private final Path basePath;
    // 保留最后多少个文件。
    private final int remainCount;
    // 总的循环操作次数
    private final int loopTimes;
    private byte[] bytes;
    private BlockingQueue<Path> stateStoreQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Path> deleteQueue = new LinkedBlockingQueue<>();
    private ExecutorService es = Executors.newFixedThreadPool(2);
    private AtomicBoolean stopped = new AtomicBoolean(false);

    public BosConflictTest(FileSystem mainFs, FileSystem stateStoreFs, FileSystem deleteFS,
                           Path basePath, int remainCount, int loopTimes) {
        this.mainFs = mainFs;
        this.stateStoreFs = stateStoreFs;
        this.deleteFS = deleteFS;
        this.basePath = basePath;
        this.remainCount = remainCount;
        this.loopTimes = loopTimes;
        this.bytes = new byte[1024];
        new Random().nextBytes(bytes);
    }

    private class DeleteTask implements Runnable {
        public void run() {
            while(! stopped.get()) {
                try {
                    Path path = deleteQueue.take();
                    System.out.println("delete task take: " + path);
                    deleteFS.delete(path, true);
                } catch(Exception e) {
                    e.printStackTrace();
                    stopped.set(true);
                }
            }
        }
    }

    private class StateStoreTask implements Runnable {
        public void run() {
            while(! stopped.get()) {
                try {
                    Path path = stateStoreQueue.take();
                    System.out.println("state store task take: " + path);
                    Path file = new Path(path, "attempt");
                    if (stateStoreFs.exists(file)) {
                        stateStoreFs.delete(file);
                    }
                    Path tmpFile = new Path(file.getParent(), file.getName() + ".tmp");
                    OutputStream out = stateStoreFs.create(tmpFile, true);
                    out.write(bytes);
                    out.close();
                    deleteQueue.offer(path, 10, TimeUnit.SECONDS);
                    stateStoreFs.rename(tmpFile, file);
                    System.out.println("rename " + tmpFile + " to " + file);
                } catch (FileNotFoundException e){
                    // ignore
                } catch (Exception e) {
                    e.printStackTrace();
                    stopped.set(true);
                }
            }
        }
    }

    public void run() throws IOException {
        es.submit(new StateStoreTask());
        es.submit(new DeleteTask());
        for (int i = 0; i < loopTimes && ! stopped.get(); i++) {
            executeOneLoop();
        }
        es.shutdownNow();
    }

    public void executeOneLoop() {
        for (int i = 0; i < remainCount && ! stopped.get(); i++) {
            try {
                Path appPath = new Path(basePath, "app-" + i);
                mainFs.mkdirs(appPath);

                Path attemptPath  = new Path(appPath, "attempt");
                OutputStream out = mainFs.create(attemptPath);
                out.write(bytes);
                out.close();

                this.stateStoreQueue.offer(appPath, 10, TimeUnit.SECONDS);

                System.out.println("offer: " + appPath);
            } catch (Exception e) {
                stopped.set(true);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            printUsage();
            System.exit(1);
        }

        LOG.info(String.format("basePath='%s', loopTime=%s", args[0], args[1]));

        URI uri = URI.create(args[0]);
        Path basePath = new Path(uri.getPath());
        int remainCount = Integer.parseInt(args[1]);
        int loopTime = Integer.parseInt(args[2]);

        Configuration conf = new HdfsConfiguration();
        String scheme = uri.getScheme();
        if (scheme == null) {
            scheme = FileSystem.getDefaultUri(conf).getScheme();
        }
        if (scheme != null) {
            String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
            conf.setBoolean(disableCacheName, true);
        }
        FileSystem mainFs = FileSystem.get(uri, conf);
        if (mainFs.exists(basePath)) {
            mainFs.delete(basePath, true);
        }
        mainFs.mkdirs(basePath);

        FileSystem stateStoreFs = FileSystem.get(uri, conf);
        FileSystem deleteFs = FileSystem.get(uri, conf);
        System.out.println("mainFs" + mainFs);
        System.out.println("stateStoreFs" + stateStoreFs);
        System.out.println("deleteFs" + deleteFs);
        BosConflictTest makeDirs = new BosConflictTest(mainFs, stateStoreFs, deleteFs, basePath, remainCount, loopTime);
        try {
            makeDirs.run();
        } finally {
            mainFs.close();
            stateStoreFs.close();
            deleteFs.close();
        }
        System.out.println("BosConflictTest over");
    }

    private static void printUsage() {
        System.out.println("BosConflictTest path remainCount loopTimes");
    }
}
