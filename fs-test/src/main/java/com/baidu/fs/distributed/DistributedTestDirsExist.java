package com.baidu.fs.distributed;

import com.baidu.fs.parallel.ParallelTestDirsExist;
import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 分布式并发按序 exists 测试工具。
 *
 * <p>本工具基于 MapReduce，起 {@code --maps} 个 mapper-only 任务，每个 mapper 调用
 * {@link ParallelTestDirsExist#run(Parameters)}，在 {@code --parallel} 个线程内按序对
 * {@code fileNumPerThread} 个文件调用 {@code fs.exists()}。所有 mapper 通过
 * {@code --sleepTime} 定义的 barrier 尽量同时起跑，模拟真实并发。</p>
 *
 * <p>使用方法：
 * <pre>
 * hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedTestDirsExist \
 *   --maps 10 \
 *   --sleepTime 60000 \
 *   --baseDir bos://yqk-bj/distributed_test \
 *   --parameters "--parallel 100 --fileNumPerThread 100 --useSubDir true"
 * </pre>
 * </p>
 *
 * <p>与 {@link DistributedReadWriteByPercent} 结构一致：每个 mapper 的实际 basePath 会被覆写为
 * {@code baseDir/map-<taskID>}，避免不同 mapper 相互干扰；若需要复用其它工具（例如
 * {@link com.baidu.fs.parallel.ParallelWriteTest}）预先写入的目录，请把
 * {@code --parameters} 中的 {@code basePath} 指到已存在的位置，并加上 {@code --overrideBasePath false}
 * 让 mapper 不再重写子目录。</p>
 *
 * @see ParallelTestDirsExist
 * @see DistributedReadWriteByPercent
 */
public class DistributedTestDirsExist {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedTestDirsExist.class);

    private static final String CONTROL_DIR_NAME = "control";
    private static final String OUTPUT_DIR_NAME = "output";
    private static final String PARAMETERS = "test.parameters";
    private static final String START_TIME = "test.starttime";
    private static final String OVERRIDE_BASE_PATH = "overrideBasePath";

    private final Configuration conf = new Configuration();

    public enum DistributedTestDirsExistCounter {
        TimeUSED,
        ExistCount,
        NotExistCount
    }

    /**
     * MapReduce 作业的 Mapper，负责单机侧的并发按序 exists 测试。
     */
    public static class TestDirsExistMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            try {
                Configuration conf = context.getConfiguration();
                String basePath = conf.get("baseDir");
                String[] args = conf.get(PARAMETERS).split(" ");
                Parameters p = Parameters.get(args);
                boolean overrideBasePath = p.getBoolean(OVERRIDE_BASE_PATH, true);
                if (overrideBasePath) {
                    p.set(ParallelTestDirsExist.BASE_PATH,
                        new Path(new Path(basePath),
                            "map-" + context.getTaskAttemptID().getTaskID()).toString());
                }
                barrier(conf);
                long beginTime = System.currentTimeMillis();
                long[] result = ParallelTestDirsExist.run(p);
                long timeUsed = System.currentTimeMillis() - beginTime;
                context.getCounter(DistributedTestDirsExistCounter.TimeUSED).increment(timeUsed);
                context.getCounter(DistributedTestDirsExistCounter.ExistCount).increment(result[0]);
                context.getCounter(DistributedTestDirsExistCounter.NotExistCount).increment(result[1]);
            } finally {
                cleanup(context);
            }
        }

        /**
         * 同步屏障：sleep 到全局起跑时刻，让所有 mapper 尽量同时开始。
         */
        private boolean barrier(Configuration conf) {
            long startTime = conf.getLong(START_TIME, 0L);
            long currentTime = System.currentTimeMillis();
            long sleepTime = startTime - currentTime;
            if (sleepTime > 0) {
                LOG.info("Waiting in barrier for: " + sleepTime + " ms");
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    private int numberOfMaps = 1;
    private String baseDir = "/tmp";
    private Parameters paras;

    public int run(String[] args) throws Exception {
        parseInputs(args);
        cleanupBeforeTestrun();
        createControlFiles();
        return runTests();
    }

    private void parseInputs(String[] args) {
        paras = Parameters.get(args);
        numberOfMaps = paras.getInt("maps", 10);
        baseDir = paras.get("baseDir");
        if (baseDir == null) {
            throw new IllegalArgumentException("--baseDir is required");
        }
        String parameters = paras.get("parameters");
        if (parameters == null) {
            throw new IllegalArgumentException("--parameters is required");
        }
        long sleepTime = paras.getLong("sleepTime", 1000);
        conf.set("maps", String.valueOf(numberOfMaps));
        conf.set(PARAMETERS, parameters);
        conf.set(START_TIME, String.valueOf(System.currentTimeMillis() + sleepTime));
        conf.set("baseDir", baseDir);
        LOG.info("numberOfMaps = {}", numberOfMaps);
        LOG.info("baseDir = {}", baseDir);
        LOG.info("parameters = {}", parameters);
        LOG.info("sleepTime = {}", sleepTime);
    }

    private void cleanupBeforeTestrun() throws IOException {
        FileSystem tempFS = FileSystem.get(new Path(baseDir).toUri(), conf);
        tempFS.delete(new Path(baseDir, CONTROL_DIR_NAME), true);
        tempFS.delete(new Path(baseDir, OUTPUT_DIR_NAME), true);
    }

    private void createControlFiles() throws IOException {
        LOG.info("Creating " + numberOfMaps + " control files");
        for (int i = 0; i < numberOfMaps; i++) {
            String strFileName = "NNBench_Controlfile_" + i;
            Path filePath = new Path(new Path(baseDir, CONTROL_DIR_NAME), strFileName);
            try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(filePath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(LongWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE))) {
                writer.append(new Text(strFileName), new LongWritable(i));
            }
        }
    }

    private int runTests() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Distributed Test Dirs Exist");
        job.setMapSpeculativeExecution(false);
        job.getConfiguration().set("mapreduce.task.timeout", "6000000");
        job.getConfiguration().set("mapreduce.task.stuck.timeout-ms", "6000000");
        job.setJarByClass(DistributedTestDirsExist.class);
        job.setMapperClass(TestDirsExistMapper.class);
        job.setNumReduceTasks(0);
        job.setMaxMapAttempts(1);
        FileInputFormat.addInputPath(job, new Path(baseDir, CONTROL_DIR_NAME));
        FileOutputFormat.setOutputPath(job, new Path(baseDir, OUTPUT_DIR_NAME));
        boolean success = job.waitForCompletion(true);

        long timeUsed = job.getCounters()
            .findCounter(DistributedTestDirsExistCounter.TimeUSED).getValue();
        long existCount = job.getCounters()
            .findCounter(DistributedTestDirsExistCounter.ExistCount).getValue();
        long notExistCount = job.getCounters()
            .findCounter(DistributedTestDirsExistCounter.NotExistCount).getValue();

        int maps = conf.getInt("maps", 10);
        String[] args = conf.get(PARAMETERS).split(" ");
        Parameters p = Parameters.get(args);
        int parallel = p.getInt(ParallelTestDirsExist.PARALLEL);
        int fileNumPerThread = p.getInt(ParallelTestDirsExist.FILE_NUM_PER_THREAD, 100);
        long totalOps = (long) maps * parallel * fileNumPerThread;
        // timeUsed 是所有 mapper 累加的毫秒数，除以 maps 得到平均单 mapper 耗时(秒)，
        // 因为 mapper 并发执行，近似等于墙钟秒数。
        long avgSeconds = Math.max(1L, timeUsed / 1000 / Math.max(1, maps));
        LOG.info("success={}, timeUsed={} ms (sum across mappers), maps={}, parallel={}, "
                + "fileNumPerThread={}, totalOps={}, existCount={}, notExistCount={}, "
                + "speed={} ops/s",
            success, timeUsed, maps, parallel, fileNumPerThread, totalOps,
            existCount, notExistCount, totalOps / avgSeconds);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new DistributedTestDirsExist().run(args);
        // 让任一 mapper 失败时进程以非零退出，方便脚本/CI 上层感知
        System.exit(exitCode);
    }
}
