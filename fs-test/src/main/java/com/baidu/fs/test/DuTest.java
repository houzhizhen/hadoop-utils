package com.baidu.fs.test;

import java.io.File;
public class DuTest {
public static void main(String[] args) {
    if (args.length != 1) {
        throw new IllegalArgumentException("Must has a parameter");
    }
    File file = new File(args[0]);
    System.out.println(file.getUsableSpace());
}
}
