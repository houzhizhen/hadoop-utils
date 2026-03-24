package com.baidu.fs.raw;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

public class Utils {

public static long parseSize(String sizeStr) {
    sizeStr = sizeStr.trim().toUpperCase();
    long multiplier = 1;

    if (sizeStr.endsWith("KB")) {
        multiplier = 1024;
        sizeStr = sizeStr.substring(0, sizeStr.length() - 2);
    } else if (sizeStr.endsWith("MB")) {
        multiplier = 1024 * 1024;
        sizeStr = sizeStr.substring(0, sizeStr.length() - 2);
    } else if (sizeStr.endsWith("GB")) {
        multiplier = 1024 * 1024 * 1024;
        sizeStr = sizeStr.substring(0, sizeStr.length() - 2);
    } else if (sizeStr.endsWith("B")) {
        sizeStr = sizeStr.substring(0, sizeStr.length() - 1);
    }

    return Long.parseLong(sizeStr) * multiplier;
}

public static long parseTime(String timeStr) {
    timeStr = timeStr.trim().toLowerCase();
    long multiplier = 1000;

    if (timeStr.endsWith("s")) {
        multiplier = 1000;
        timeStr = timeStr.substring(0, timeStr.length() - 1);
    } else if (timeStr.endsWith("m")) {
        multiplier = 60 * 1000;
        timeStr = timeStr.substring(0, timeStr.length() - 1);
    } else if (timeStr.endsWith("h")) {
        multiplier = 60 * 60 * 1000;
        timeStr = timeStr.substring(0, timeStr.length() - 1);
    } else if (timeStr.endsWith("ms")) {
        multiplier = 1;
        timeStr = timeStr.substring(0, timeStr.length() - 2);
    }

    return Long.parseLong(timeStr) * multiplier;
}

public static String formatSize(long bytes) {
    if (bytes >= 1024 * 1024 * 1024) {
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    } else if (bytes >= 1024 * 1024) {
        return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
    } else if (bytes >= 1024) {
        return String.format("%.2f KB", bytes / 1024.0);
    } else {
        return bytes + " B";
    }
}

public static String formatTime(long millis) {
    if (millis >= 60 * 60 * 1000) {
        return String.format("%.2f 小时", millis / (60.0 * 60.0 * 1000.0));
    } else if (millis >= 60 * 1000) {
        return String.format("%.2f 分钟", millis / (60.0 * 1000.0));
    } else if (millis >= 1000) {
        return String.format("%.2f 秒", millis / 1000.0);
    } else {
        return millis + " 毫秒";
    }
}

public static void createTestFile(FileSystem fs, Path path, long fileSize) throws IOException {
    fs.mkdirs(path.getParent());
    byte[] buffer = new byte[4096];
    for (int i = 0; i < buffer.length; i++) {
        buffer[i] = (byte) (i % 256);
    }
    OutputStream out = fs.create(path, true);
    long remaining = fileSize;
    while (remaining > 0) {
        int bytesToWrite = (int) Math.min(buffer.length, remaining);
        out.write(buffer, 0, bytesToWrite);
        remaining -= bytesToWrite;
    }
    out.close();
}
}
