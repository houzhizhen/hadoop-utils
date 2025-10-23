package com.baidu.fs.parallel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.Arrays;

public class DfsRouterMountTableTest  extends Configured implements Tool  {

private String operator;
private String authority;
private Path basePath;
private Configuration conf;
private FileSystem fs;
private  RouterAdmin admin;
private String[] adminArgs;

public DfsRouterMountTableTest() {

}


public static void main(String[] argv) throws Exception {
    if (argv.length != 2) {
        System.err.println("Must has basePath, for example -add hdfs://bmr-cluster/testroute, or -rm hdfs://bmr-cluster/testroute");
        System.exit(1);
    }
    DfsRouterMountTableTest dfsRouterMountTableTest = new DfsRouterMountTableTest();
    int res = ToolRunner.run(dfsRouterMountTableTest, argv);
    System.exit(res);
}

@Override
public int run(String[] args) throws Exception {
    URI uri = URI.create(args[1]);
    this.authority = uri.getAuthority();
    operator = args[0];
    if ("-add".equals(operator)) {
        adminArgs= new String[]{"-add", "mount path", this.authority, "path"};
    } else if ("-rm".equals(operator)) {
        adminArgs= new String[]{"-rm", "mount path"};
    } else {
        throw new RuntimeException("Unknown operator " + operator + ", expected -add or -rm");
    }
    this.basePath = new Path(uri.getPath());
    this.conf = new HdfsConfiguration();
    this.admin = new RouterAdmin(conf);
    this.fs = FileSystem.get(conf);
    if (fs.exists(basePath)) {
        long beginTime = System.currentTimeMillis();
        recursive(basePath);
        long timeUsed = System.currentTimeMillis() - beginTime;
        System.out.println(operator + " used " + timeUsed + " ms");
        return 0;
    } else {
        System.err.println("Basedir " + basePath + " does not exist");
        return 1;
    }

}

private void recursive(Path path) throws Exception {
    FileStatus[] statuses = fs.listStatus(path);
    if (statuses.length == 0) {
        String pathStr = path.toUri().getRawPath();
        if ("-add".equals(operator)) {
            adminArgs[1] = pathStr;
            adminArgs[3] = pathStr;
        } else {
            adminArgs[1] = pathStr;
        }

        System.out.println("args:" + Arrays.toString(adminArgs));
        admin.run(adminArgs);
    } else {
        for(FileStatus status : statuses) {
            if (status.isDirectory()) {
                recursive(status.getPath());
            }
        }
    }
}
}
