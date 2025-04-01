package com.baidu.java.test;

import org.apache.hadoop.security.SecurityUtil;

import java.io.IOException;

public class TestMain {

    public static void main(String[] args) throws IOException {

        long base = 1L;
        base *= 1000 * 1000 * 1000 * 1000L;
        System.out.println(base);
        String principal = "sts/_HOST@BAIDU.COM";
        System.out.println(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"));
    }

}
