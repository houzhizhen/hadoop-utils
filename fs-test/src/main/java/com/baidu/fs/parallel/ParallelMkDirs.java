package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 并发、按顺序执行 mkdir 的压测工具。
 *
 * <p>启动 {@code --parallel} 个线程，每个线程内按序调用 {@code fs.mkdirs()}
 * 创建 {@code fileNumPerThread} 个目录：</p>
 * <ul>
 *   <li>{@code --useSubDir true}（默认）: 每线程一个子目录 {@code basePath/sub<threadNum>/dir-<i>}，
 *       与 {@link ParallelWriteTest} / {@link ParallelTestDirsExist} 的路径约定对齐；</li>
 *   <li>{@code --useSubDir false}: 全部平铺在 {@code basePath} 下，路径为
 *       {@code basePath/dir-<threadNum>-<i>}，用于测 NN 单目录锁竞争。</li>
 * </ul>
 *
 * <p>任一线程抛 {@link IOException} 会让所有线程尽快停下，最终把首个异常再抛出，
 * 便于 MR mapper 或上层脚本以失败状态退出。{@code mkdirs()} 返回 {@code false}
 * 不视为错误（HDFS 很少走到这种分支），只要不抛异常就累计到 {@code mkDirsCount}。</p>
 *
 * @see ParallelTestDirsExist
 * @see com.baidu.fs.distributed.DistributedMkDirs
 */
public class ParallelMkDirs {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelMkDirs.class);

    public static final String PARALLEL = "parallel";
    public static final String BASE_PATH = "basePath";
    public static final String FILE_NUM_PER_THREAD = "fileNumPerThread";
    public static final String USE_SUB_DIR = "useSubDir";
    public static final String SUB_DIR_PREFIX = "subDirPrefix";
    public static final String DIR_PREFIX = "dirPrefix";

    /**
     * 执行一轮并发 mkdir 压测。
     *
     * @return 长度为 1 的数组：{@code [mkDirsCount]}，累计成功调用 {@code mkdirs()} 的次数，
     *         供分布式版本聚合到 counter
     */
    public static long[] run(Parameters parameters) throws InterruptedException, IOException {
        int parallel = parameters.getInt(PARALLEL);
        Path basePath = new Path(parameters.get(BASE_PATH));
        int fileNumPerThread = parameters.getInt(FILE_NUM_PER_THREAD, 100);
        boolean useSubDir = parameters.getBoolean(USE_SUB_DIR, true);
        String subDirPrefix = parameters.get(SUB_DIR_PREFIX, "sub");
        String dirPrefix = parameters.get(DIR_PREFIX, "dir-");

        LOG.info("parallel={}", parallel);
        LOG.info("basePath={}", basePath);
        LOG.info("fileNumPerThread={}", fileNumPerThread);
        LOG.info("useSubDir={}", useSubDir);
        LOG.info("subDirPrefix={}", subDirPrefix);
        LOG.info("dirPrefix={}", dirPrefix);

        FileSystem fs = FileSystem.get(basePath.toUri(), new Configuration());
        // 兜底：确保根目录存在；不主动删除既有子树，避免误删用户数据
        fs.mkdirs(basePath);

        CountDownLatch latch = new CountDownLatch(parallel);
        ExecutorService es = Executors.newFixedThreadPool(parallel);
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicReference<Throwable> eCatch = new AtomicReference<>();
        AtomicLong mkDirsCount = new AtomicLong();

        long beginTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < parallel; i++) {
                final int threadNum = i;
                es.submit(() -> {
                    try {
                        Path threadDir = useSubDir
                            ? new Path(basePath, subDirPrefix + threadNum)
                            : basePath;
                        for (int dirNum = 0;
                             dirNum < fileNumPerThread && !stopped.get();
                             dirNum++) {
                            Path dirPath = useSubDir
                                ? new Path(threadDir, dirPrefix + dirNum)
                                : new Path(basePath, dirPrefix + threadNum + "-" + dirNum);
                            fs.mkdirs(dirPath);
                            mkDirsCount.incrementAndGet();
                        }
                    } catch (Throwable t) {
                        stopped.set(true);
                        eCatch.compareAndSet(null, t);
                        LOG.warn("thread {} failed", threadNum, t);
                    } finally {
                        latch.countDown();
                    }
                });
            }
        } finally {
            es.shutdown();
        }
        latch.await(1, TimeUnit.HOURS);
        long timeUsed = System.currentTimeMillis() - beginTime;
        LOG.info("ParallelMkDirs finished, {} ms used, mkDirsCount={}",
            timeUsed, mkDirsCount.get());
        Throwable t = eCatch.get();
        if (t != null) {
            // 让上层（例如 MR mapper）以失败状态退出，而不是"看起来成功"。
            throw new IOException("ParallelMkDirs failed: " + t, t);
        }
        return new long[] {mkDirsCount.get()};
    }

    public static void main(String[] args) throws Exception {
        run(Parameters.get(args));
    }
}
