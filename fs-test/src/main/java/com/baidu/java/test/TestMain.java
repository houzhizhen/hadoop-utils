package com.baidu.java.test;

import org.apache.hadoop.HadoopIllegalArgumentException;

import java.io.IOException;

public class TestMain {
    public static void main(String[] args) throws IOException {

       for (int i = 1; i <= 1024; i++) {
           long size = i * 1024L * 1024 * 1024; // iG
           int capacity = computeCapacity(size, 2.0D, "");
           System.out.println(i + "G memory capacity = " + capacity);
       }
    }

static int computeCapacity(long maxMemory, double percentage,
                           String mapName) {
    if (percentage > 100.0 || percentage < 0.0) {
        throw new HadoopIllegalArgumentException("Percentage " + percentage
            + " must be greater than or equal to 0 "
            + " and less than or equal to 100");
    }
    if (maxMemory < 0) {
        throw new HadoopIllegalArgumentException("Memory " + maxMemory
            + " must be greater than or equal to 0");
    }
    if (percentage == 0.0 || maxMemory == 0) {
        return 0;
    }
    //VM detection
    //See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
    final String vmBit = System.getProperty("sun.arch.data.model");

    //Percentage of max memory
    final double percentDivisor = 100.0/percentage;
    final double percentMemory = maxMemory/percentDivisor;

    //compute capacity
    final int e1 = (int)(Math.log(percentMemory)/Math.log(2.0) + 0.5);
    final int e2 = e1 - ("32".equals(vmBit)? 2: 3);
    final int exponent = e2 < 0? 0: e2 > 30? 30: e2;
    final int c = 1 << exponent;
    return c;
}

}
