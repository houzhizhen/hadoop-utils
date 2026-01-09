package com.baidu.fs.util;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Callable;

public class CpuTimer {
public static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

public static <T> T measureCpuTime(Callable<T> action) throws Exception {
    OperatingSystemMXBean osBean =
        (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    long startCpu = osBean.getProcessCpuTime();
    long startWall = System.nanoTime();

    // 执行任意 lambda
    T result = action.call();

    long endCpu = osBean.getProcessCpuTime();
    long endWall = System.nanoTime();

    double cpuMs = (endCpu - startCpu) / 1_000_000.0;
    double wallMs = (endWall - startWall) / 1_000_000.0;

    LOG.info("CPU Time: {} ms", cpuMs);
    LOG.info("Wall Time: {} ms", wallMs);
    LOG.info("CPU Usage:{} ", 100.0 * cpuMs / wallMs);

    return result;
}
}