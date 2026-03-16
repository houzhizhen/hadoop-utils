package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ParallelReadWriteByPercent {

public static void main(String[] args) throws IOException, InterruptedException {
    Parameters parameters = Parameters.get(args);
    int parallel = parameters.getInt("parallel");
    double readPercent = parameters.getDouble("readPercent", 0.0);
    int readParallel = (int) (parallel * readPercent);
    int writeParallel = parallel - readParallel;
    Path basePath = new Path(parameters.get("basePath"));
    System.out.println("readParallel:" + readParallel);
    System.out.println("writeParallel:" + writeParallel);
    System.out.println("basePath:" + basePath);
    FileSystem fs = FileSystem.get(basePath.toUri(), new Configuration());
    if (fs.exists(basePath)) {
        fs.delete(basePath, true);
    }
    fs.mkdirs(basePath);

    CountDownLatch latch = new CountDownLatch(parallel);
    Parameters readParameters = new Parameters(parameters);
    readParameters.set("baseDir", new Path(basePath, "read").toString());
    readParameters.set("parallel", readParallel + "");
    Parameters writeParameters = new Parameters(parameters);
    writeParameters.set("basePath", new Path(basePath, "write").toString());
    writeParameters.set("parallel", writeParallel + "");
    long beginTime = System.currentTimeMillis();
    if (readParallel > 0) {
        new ParallelReadTest(readParameters, latch).start();
    }
    if (writeParallel > 0) {
        new ParallelWriteTest(writeParameters, latch).start();
    }

    latch.await(1, TimeUnit.HOURS);
    long timeUsed = System.currentTimeMillis() - beginTime;
    System.out.println("ParallelReadWrite finished, " + timeUsed + " ms used.");
}
}
