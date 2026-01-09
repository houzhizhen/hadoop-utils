package com.baidu.hadoop.io;

import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class TestIOUtils {
public static void main(String[] args) throws IOException {
System.out.println(IOUtils.listDirectory(
    new File("/Users/houzhizhen/git/hadoop-utils"), new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return true;
        }
    }));
}
}
