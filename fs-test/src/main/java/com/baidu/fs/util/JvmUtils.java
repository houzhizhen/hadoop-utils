package com.baidu.fs.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class JvmUtils {
public static long getCurrentGcTime() {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    long totalGcTime = 0L;
    for (GarbageCollectorMXBean bean : gcBeans) {

        totalGcTime += bean.getCollectionTime();
    }
    return totalGcTime;
}
}
