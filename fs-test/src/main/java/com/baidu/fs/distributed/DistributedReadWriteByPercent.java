package com.baidu.fs.distributed;

import com.baidu.fs.parallel.ParallelReadWriteByPercent;
import com.baidu.fs.util.Parameters;
import org.apache.hadoop.HadoopIllegalArgumentException;
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
 * 分布式读写比例测试工具
 *
 * <p>这是一个MapReduce作业，用于分布式执行ParallelReadWriteByPercent测试。
 * 它允许在多个节点上同时运行读写比例测试，模拟真实生产环境中的并发负载。</p>
 *
 * <p>使用方法：
 * <pre>
 * hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedReadWriteByPercent \
 *   -maps 2 \
 *   -baseDir /tmp/distributed_test \
 *   -parameters "--parallel 10 --readPercent 0.7 --basePath /tmp/test_data"
 * </pre>
 * </p>
 *
 * @author Zulu
 * @version 1.0
 * @see ParallelReadWriteByPercent
 * @see DistributedReadTest
 */
public class DistributedReadWriteByPercent {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedReadWriteByPercent.class);

    private static final String CONTROL_DIR_NAME = "control";
    private static final String OUTPUT_DIR_NAME = "output";
    private static final String PARAMETERS = "test.parameters";
    public static final String BASE_DIR = "test.basedir";
    private static final String START_TIME = "test.starttime";

    private final Configuration conf = new Configuration();
    public static enum DistributedReadWriteByPercentCounter {
        TimeUSED
    }
    /**
     * MapReduce作业的Mapper类，负责执行实际的读写测试。
     *
     * <p>每个Map任务会调用ParallelReadWriteByPercent.main()方法执行测试，
     * 使用barrier机制确保所有任务同时开始。</p>
     */
    public static class ReadWriteTestMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * Mapper的主要执行方法，负责调用具体的测试逻辑。
         *
         * @param context MapReduce任务上下文
         * @throws IOException 如果发生I/O错误
         * @throws InterruptedException 如果任务被中断
         */
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            try {
                Configuration conf = context.getConfiguration();
                String[] args = conf.get(PARAMETERS).split(" ");
                Parameters p = Parameters.get(args);
                barrier(conf);
                long beginTime = System.currentTimeMillis();
                p.set(ParallelReadWriteByPercent.BASE_PATH, new Path(new Path(p.get(ParallelReadWriteByPercent.BASE_PATH)),
                    "map-" + context.getTaskAttemptID().getTaskID()).toString());
                ParallelReadWriteByPercent.run(p);
                long endTime = System.currentTimeMillis();
                long timeUsed = endTime-beginTime;
                context.getCounter(DistributedReadWriteByPercentCounter.TimeUSED).increment(timeUsed);
            } finally {
                cleanup(context);
            }
        }

        /**
         * 同步屏障机制，确保所有Map任务在同一时间开始执行测试。
         *
         * @param conf Hadoop配置对象
         * @return 如果成功等待则返回true，否则返回false
         */
        private boolean barrier(Configuration conf) {
            long startTime = conf.getLong(START_TIME, 0l);
            long currentTime = System.currentTimeMillis();
            long sleepTime = startTime - currentTime;
            boolean retVal = true;

            // If the sleep time is greater than 0, then sleep and return
            if (sleepTime > 0) {
                LOG.info("Waiting in barrier for: " + sleepTime + " ms");

                try {
                    Thread.sleep(sleepTime);
                    retVal = true;
                } catch (Exception e) {
                    retVal = false;
                }
            }

            return retVal;
        }
    }

    private void createControlFiles() throws IOException {
        LOG.info("Creating " + numberOfMaps + " control files");

        for (int i = 0; i < numberOfMaps; i++) {
            String strFileName = "NNBench_Controlfile_" + i;
            Path filePath = new Path(new Path(baseDir, CONTROL_DIR_NAME),
                strFileName);

            SequenceFile.Writer writer = null;
            try {
                writer = SequenceFile.createWriter(getConf(), SequenceFile.Writer.file(filePath),
                    SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(LongWritable.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
                writer.append(new Text(strFileName), new LongWritable(i));
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    /**
     * 主要的运行方法，负责整个测试流程的控制。
     *
     * @param args 命令行参数
     * @return 退出码
     * @throws Exception 如果测试过程中发生错误
     */
    public int run(String[] args) throws Exception {
        // Display the application version string
        // Parse the inputs
        parseInputs(args);

        // Clean up files before the test run
        cleanupBeforeTestrun();

        // Create control files before test run
        createControlFiles();

        // Run the tests as a map reduce job
        runTests();

        // Analyze results
        return 0;
    }

    private void cleanupBeforeTestrun() throws IOException {
        FileSystem tempFS = FileSystem.get(new Path(baseDir).toUri(), getConf());

        tempFS.delete(new Path(baseDir, CONTROL_DIR_NAME), true);
        tempFS.delete(new Path(baseDir, OUTPUT_DIR_NAME), true);
    }

    private Configuration getConf() {
        return this.conf;
    }

    private int numberOfMaps = 1;
    private String baseDir = "/tmp";

    private void parseInputs(String[] args) {
        Parameters paras = Parameters.get(args);
        numberOfMaps = paras.getInt("maps", 10);
        baseDir = paras.get("baseDir");
        String parameters = paras.get("parameters");
        long sleepTime = paras.getLong("sleepTime", 1000);
        conf.set(PARAMETERS, parameters);
        conf.set(START_TIME, String.valueOf(System.currentTimeMillis() + sleepTime));
        LOG.info("numberOfMaps = {}", numberOfMaps);
        LOG.info("baseDir = {}", baseDir);
        LOG.info("parameters = {}", parameters);
        LOG.info("sleepTime = {}", sleepTime);
        LOG.info("parameters = {}", paras.toString() );
    }


    private void runTests() throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "Distributed ReadWrite Test By Percent");
        job.setMapSpeculativeExecution(false);
        job.getConfiguration().set("mapreduce.task.timeout", "6000000");
        job.getConfiguration().set("mapreduce.task.stuck.timeout-ms", "6000000");
        job.setJarByClass(DistributedReadWriteByPercent.class);
        job.setMapperClass(ReadWriteTestMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(baseDir, CONTROL_DIR_NAME));
        FileOutputFormat.setOutputPath(job, new Path(baseDir, OUTPUT_DIR_NAME));
        job.waitForCompletion(true);
    }

    /**
     * 程序入口点。
     *
     * @param args 命令行参数
     * @throws Exception 如果测试过程中发生错误
     */
    public static void main(String[] args) throws Exception {
        new DistributedReadWriteByPercent().run(args);
    }
}