package com.baidu.fs.parallel;

import com.baidu.fs.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ParallelReadWriteByPercent {

public static final String PARALLEL = "parallel";
public static final String BASE_PATH = "basePath";
public static void main(String[] args) throws IOException, InterruptedException {
    Parameters parameters = Parameters.get(args);
    run(parameters);

}
public static void run(Parameters parameters) throws InterruptedException, IOException {
    int parallel = parameters.getInt(PARALLEL);
    double readPercent = parameters.getDouble("readPercent", 0.0);
    int readParallel = (int) (parallel * readPercent);
    int writeParallel = parallel - readParallel;
    Path basePath = new Path(parameters.get(BASE_PATH));
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
    ParallelReadTest readTest = null;
    ParallelWriteTest writeTest = null;
    if (readParallel > 0) {
        readTest = new ParallelReadTest(readParameters, latch);
        readTest.start();
    }
    if (writeParallel > 0) {
        writeTest = new ParallelWriteTest(writeParameters, latch);
        writeTest.start();
    }

    latch.await(1, TimeUnit.HOURS);
    long timeUsed = System.currentTimeMillis() - beginTime;
    System.out.println("ParallelReadWrite finished, " + timeUsed + " ms used.");
    // 把子测试里遇到的第一个异常向上传播，避免"看起来成功"。
    if (readTest != null) {
        readTest.checkFailure();
    }
    if (writeTest != null) {
        writeTest.checkFailure();
    }
}
}
