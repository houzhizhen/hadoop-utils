package com.baidu.fs.router;

import com.baidu.fs.parallel.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MountRecursiveDirs extends Configured implements Tool {
public MountRecursiveDirs(Configuration conf) {
    super(conf);
}

public static void main(String[] argv) throws Exception {
    Configuration conf = new HdfsConfiguration();
    MountRecursiveDirs admin = new MountRecursiveDirs(conf);

    int res = ToolRunner.run(admin, argv);
    System.exit(res);
}

private static void printUsage() {
    System.out.println("Usage: List threadNum location iterationTimes expectedCount");
}

@Override
public int run(String[] args) throws Exception {
    return 0;
}
}
