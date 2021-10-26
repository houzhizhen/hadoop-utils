package com.baidu.fs.raw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;

public class RawFs {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            printUsage();
            System.exit(1);
        } else {
            Configuration conf = new HdfsConfiguration();
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, args.length - 1);
            Command command = CommandFactory.getCommand(conf, args[0]);
            command.exec(newArgs);
        }
    }

    private static void printUsage() {
        System.out.println("usage: RawFs command parameters");
    }
}
