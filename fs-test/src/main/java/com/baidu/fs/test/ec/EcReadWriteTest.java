package com.baidu.fs.test.ec;

import com.baidu.fs.util.FileReader;
import com.baidu.fs.util.FileWriter;
import com.baidu.fs.util.Parameters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class EcReadWriteTest {
public static void main(String[] args) throws IOException {
    Parameters parameters = new Parameters(args);
    String[] paths = parameters.getArray("paths");
    long fileLength = parameters.getLong("file-length");
    if (paths == null || paths.length < 1) {
        throw new RuntimeException("Must has parameter paths");
    }
    HdfsConfiguration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(URI.create(paths[0]), conf);

    for (String path : paths) {
        FileWriter.write(fs, new Path(path), fileLength);
        FileReader.readFile(fs, new Path(path), fileLength);
    }
}
}
