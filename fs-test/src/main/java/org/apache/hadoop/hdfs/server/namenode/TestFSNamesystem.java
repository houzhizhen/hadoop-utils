package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.text.StringEscapeUtils;

public class TestFSNamesystem {

public static void main(String[] args) {
    String r = StringEscapeUtils.escapeJava(null);
    System.out.println("escapeJava(null) result is null " +(r == null));
    System.out.println("escapeJava(null) = " + r);
    StringBuilder sb = new StringBuilder("abc").append(r);
    System.out.println("sb = " + sb);
}
}
