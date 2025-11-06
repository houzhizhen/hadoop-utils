package com.baidu.fs.test;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

public class DuTest {

public static void main(String[] args) {
    for (int i =0; i < 10; i++) {
        System.out.println(ThreadLocalRandom.current().nextInt((int) (21600000L))/1000/60);
    }

    if (args.length != 1) {
        throw new IllegalArgumentException("Must has a parameter");
    }
    File file = new File(args[0]);
    System.out.println(file.getUsableSpace());
}
}
