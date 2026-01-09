package com.baidu.fs.util;

import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class JvmUtils {
public static long getCurrentGcTime() {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    long totalGcTime = 0L;
    for (GarbageCollectorMXBean bean : gcBeans) {

        totalGcTime += bean.getCollectionTime();
    }
    return totalGcTime;
}
public static void main(String[] args) throws IOException {
    for(int i = 0; i < 100; i++) {
        long jitter =  60000;
        System.out.println(ThreadLocalRandom.current()
            .nextLong(-jitter, jitter));
    }

}
}
