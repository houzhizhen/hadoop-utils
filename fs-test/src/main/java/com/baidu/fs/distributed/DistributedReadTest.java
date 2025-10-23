package com.baidu.fs.distributed;

import com.baidu.fs.parallel.GetFileInfoTest;
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

public class DistributedReadTest {
private static final Logger LOG = LoggerFactory.getLogger(DistributedReadTest.class);

private static final String CONTROL_DIR_NAME = "control";
private static final String OUTPUT_DIR_NAME = "output";
private static final String PARAMETERS = "test.parameters";
public static final String BASE_DIR = "test.basedir";
private static final String START_TIME = "test.starttime";

private final Configuration conf = new Configuration();

public static class ReadTestMapper
    extends Mapper<Object, Text, Text, IntWritable> {

    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            Configuration conf = context.getConfiguration();
            barrier(conf);
            String[] args = conf.getStrings(PARAMETERS);
            GetFileInfoTest.run(conf, args);
        } finally {
            cleanup(context);
        }
    }
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
private static void checkArgs(final int index, final int length) {
    if (index == length) {
        throw new HadoopIllegalArgumentException("Not enough arguments");
    }
}
private void cleanupBeforeTestrun() throws IOException {
    FileSystem tempFS = FileSystem.get(new Path(baseDir).toUri(), getConf());

    tempFS.delete(new Path(baseDir, CONTROL_DIR_NAME), true);
    tempFS.delete(new Path(baseDir, OUTPUT_DIR_NAME), true);
}

private Configuration getConf() {
    return this.conf;
}

private long numberOfMaps = 1l;
private String baseDir = "/tmp";
private String parameters = "";
private void parseInputs(String[] args) {
    for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-maps")) {
            checkArgs(i + 1, args.length);
            numberOfMaps = Long.parseLong(args[++i]);
            LOG.info("numberOfMaps:" + numberOfMaps);
        } else if (args[i].equals("-baseDir")) {

            checkArgs(i + 1, args.length);
            baseDir = args[++i];
            conf.set(BASE_DIR, baseDir);
            LOG.info("baseDir:" + baseDir);
        } else if (args[i].equals("-parameters")) {
            checkArgs(i + 1, args.length);
            parameters = args[++i];
            LOG.info("PARAMETERS:" + parameters);
            conf.set(PARAMETERS, parameters);
        }
        conf.set(START_TIME, String.valueOf(System.currentTimeMillis() + 1000));
    }
}


private void runTests() throws IOException, InterruptedException, ClassNotFoundException {

    Job job = Job.getInstance(conf, "Distributed Test");

    job.setJarByClass(DistributedReadTest.class);
    job.setMapperClass(ReadTestMapper.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(baseDir, CONTROL_DIR_NAME));
    FileOutputFormat.setOutputPath(job, new Path(baseDir, OUTPUT_DIR_NAME));
    job.waitForCompletion(true);
}

public static void main(String[] args) throws Exception {
    new DistributedReadTest().run(args);
}
}
