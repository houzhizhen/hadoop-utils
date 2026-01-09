package com.baidu.fs.test.ec;

import com.baidu.fs.util.CpuTimer;
import com.baidu.fs.util.FileReader;
import com.baidu.fs.util.FileWriter;
import com.baidu.fs.util.Parameters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.net.URI;

public class EcReadWriteTest {
public static void main(String[] args) throws Exception {
    Parameters parameters = new Parameters(args);
    String[] paths = parameters.getArray("paths");
    long fileLength = parameters.getLong("file-length");
    if (paths == null || paths.length < 1) {
        throw new RuntimeException("Must has parameter paths");
    }
    HdfsConfiguration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(URI.create(paths[0]), conf);

    for (String p : paths) {
        Path path = new Path(p);
        fs.mkdirs(path.getParent());
        CpuTimer.measureCpuTime(() -> {
            FileWriter.write(fs, path, fileLength);
            return null;
            });

        CpuTimer.measureCpuTime(() -> {
            FileReader.readFile(fs, path, fileLength);
            return null;
        });
    }
}
}
