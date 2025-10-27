package com.baidu.fs.test;

import com.baidu.fs.util.JvmUtils;
import com.baidu.fs.util.Parameters;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FilePerformanceTest {

private static byte[] ONE_BYTE = new byte[1];
private static byte[] ELEVEN_BYTE = new byte[11];

public static void main(String[] args) throws IOException {
    Parameters parameters = new Parameters(args);
    String baseDir = parameters.get("base-dr", "/tmp/abc");
    int[] dirsLevel = parameters.getIntArray("dir-num-per-level", "2,2");
    int iteratorTime = parameters.getInt("iterator-time", 10);
    createDirsInBaseDir(baseDir, dirsLevel, 0);
    for (int i = 0; i < iteratorTime; i++) {

        long startGcTime = JvmUtils.getCurrentGcTime();
        long beginTime = System.currentTimeMillis();
        testIterator(baseDir, dirsLevel, 0, i);

        long endTime = System.currentTimeMillis();
        long endGcTime = JvmUtils.getCurrentGcTime();
        long gcTime = endGcTime - startGcTime;
        long timeUsed = endTime - beginTime;
        long timeWithoutGc = timeUsed - (endGcTime - startGcTime);
        System.out.println("Iterator " + i + " use " + timeUsed + "ms, gc time " +
            gcTime + "ms, time without gc " + timeWithoutGc + "ms");
    }



}

private static void testIterator(String parentPath, int[] dirsLevel, int currentLevel, int iteratorIndex) throws IOException {
    if (currentLevel == dirsLevel.length -1) {
        for (int lastIndex = 0; lastIndex < dirsLevel[currentLevel]; lastIndex++) {
            File dir = new File(parentPath, "subdir"+lastIndex);
            File blockFile = new File(dir, "blk_" + iteratorIndex);
            OutputStream blockOs = new FileOutputStream(blockFile);
            blockOs.write(ONE_BYTE);
            blockOs.close();
            File metaFile = new File(dir, "blk_" + iteratorIndex + ".meta");
            OutputStream metaOs = new FileOutputStream(metaFile);
            metaOs.write(ELEVEN_BYTE);
            metaOs.close();
        }
    } else {
        for (int index = 0; index < dirsLevel[currentLevel]; index++) {
            File file = new File(parentPath, "subdir"+index);
            testIterator(file.getAbsolutePath(), dirsLevel, currentLevel+1, iteratorIndex);
        }
    }
}

private static void createDirsInBaseDir(String parentPath, int[] dirsLevel, int currentLevel) {
    if (currentLevel == dirsLevel.length -1) {
        for (int lastIndex = 0; lastIndex < dirsLevel[currentLevel]; lastIndex++) {
            File file = new File(parentPath, "subdir"+lastIndex);
            file.mkdirs();
        }
    } else {
        for (int index = 0; index < dirsLevel[currentLevel]; index++) {
            File file = new File(parentPath, "subdir"+index);
            file.mkdirs();
            createDirsInBaseDir(file.getAbsolutePath(), dirsLevel, currentLevel+1);
        }
    }
}
}
