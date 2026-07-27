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
 * 并发、按顺序测试文件是否存在。
 *
 * <p>启动 {@code --parallel} 个线程，每个线程内按序调用 {@code fs.exists()} 检查
 * {@code fileNumPerThread} 个文件路径：</p>
 * <ul>
 *   <li>{@code --useSubDir true}（默认）: 每线程一个子目录 {@code basePath/sub<threadNum>/file-<i>}，
 *       与 {@link ParallelWriteTest} 的写入目录结构对齐；</li>
 *   <li>{@code --useSubDir false}: 全部平铺在 {@code basePath} 下，路径为
 *       {@code basePath/file-<threadNum>-<i>}。</li>
 * </ul>
 *
 * <p>{@code exists()} 抛 {@link IOException} 会让所有线程尽快停下（避免大规模刷错误日志）；
 * 返回 {@code false} 不视为错误，只累计到 {@code notExistCount}。</p>
 *
 * @see ParallelReadTest
 * @see ParallelWriteTest
 * @see com.baidu.fs.distributed.DistributedTestDirsExist
 */
public class ParallelTestDirsExist {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelTestDirsExist.class);

    public static final String PARALLEL = "parallel";
    public static final String BASE_PATH = "basePath";
    public static final String FILE_NUM_PER_THREAD = "fileNumPerThread";
    public static final String USE_SUB_DIR = "useSubDir";
    public static final String SUB_DIR_PREFIX = "subDirPrefix";
    public static final String FILE_PREFIX = "filePrefix";

    /**
     * 执行一轮并发按序 exists 测试。
     *
     * @param parameters 测试参数
     * @return 长度为 2 的数组：{@code [existCount, notExistCount]}，供分布式版本聚合到 counter
     */
    public static long[] run(Parameters parameters) throws InterruptedException, IOException {
        int parallel = parameters.getInt(PARALLEL);
        Path basePath = new Path(parameters.get(BASE_PATH));
        int fileNumPerThread = parameters.getInt(FILE_NUM_PER_THREAD, 100);
        boolean useSubDir = parameters.getBoolean(USE_SUB_DIR, true);
        String subDirPrefix = parameters.get(SUB_DIR_PREFIX, "sub");
        String filePrefix = parameters.get(FILE_PREFIX, "file-");

        LOG.info("parallel={}", parallel);
        LOG.info("basePath={}", basePath);
        LOG.info("fileNumPerThread={}", fileNumPerThread);
        LOG.info("useSubDir={}", useSubDir);
        LOG.info("subDirPrefix={}", subDirPrefix);
        LOG.info("filePrefix={}", filePrefix);

        FileSystem fs = FileSystem.get(basePath.toUri(), new Configuration());

        CountDownLatch latch = new CountDownLatch(parallel);
        ExecutorService es = Executors.newFixedThreadPool(parallel);
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicReference<Throwable> eCatch = new AtomicReference<>();
        AtomicLong existCount = new AtomicLong();
        AtomicLong notExistCount = new AtomicLong();

        long beginTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < parallel; i++) {
                final int threadNum = i;
                es.submit(() -> {
                    try {
                        Path threadDir = useSubDir
                            ? new Path(basePath, subDirPrefix + threadNum)
                            : basePath;
                        for (int fileNum = 0;
                             fileNum < fileNumPerThread && !stopped.get();
                             fileNum++) {
                            Path filePath = useSubDir
                                ? new Path(threadDir, filePrefix + fileNum)
                                : new Path(basePath, filePrefix + threadNum + "-" + fileNum);
                            if (fs.exists(filePath)) {
                                existCount.incrementAndGet();
                            } else {
                                notExistCount.incrementAndGet();
                            }
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
        LOG.info("ParallelTestDirsExist finished, {} ms used, existCount={}, notExistCount={}",
            timeUsed, existCount.get(), notExistCount.get());
        Throwable t = eCatch.get();
        if (t != null) {
            // 让上层（例如 MR mapper）以失败状态退出，而不是"看起来成功"。
            throw new IOException("ParallelTestDirsExist failed: " + t, t);
        }
        return new long[] {existCount.get(), notExistCount.get()};
    }

    public static void main(String[] args) throws Exception {
        run(Parameters.get(args));
    }
}
