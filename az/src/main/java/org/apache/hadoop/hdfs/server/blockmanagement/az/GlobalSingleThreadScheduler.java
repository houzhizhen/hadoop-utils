package org.apache.hadoop.hdfs.server.blockmanagement.az;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalSingleThreadScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalSingleThreadScheduler.class);
  private static Map<String, Params> taskParamsMap;
  private static ScheduledExecutorService scheduledExecutorService;

  public static synchronized void schedule(String name, Runnable runnable,
      long delay, long period, TimeUnit timeUnit) {
    if (scheduledExecutorService == null) {
      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      taskParamsMap = new HashMap<>();
    }
    if (taskParamsMap.containsKey(name)) {
      LOG.info("Task {} exists, skip submit", name);
      return;
    }
    Params params = new Params(delay, period, timeUnit);
    taskParamsMap.put(name, params);
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        runnable.run();
      } catch (Exception e) {
        LOG.warn("Run task {} failed", name, e);
      }
    }, delay, period, timeUnit);
    LOG.info("Add task {} to schedule, params is {}, current schedule tasks are {}", name,
        params, taskParamsMap);
  }

  public static class Params {

    private final long delay;
    private final long period;
    private final TimeUnit timeUnit;


    public Params(long delay, long period, TimeUnit timeUnit) {
      this.delay = delay;
      this.period = period;
      this.timeUnit = timeUnit;
    }

    @Override
    public String toString() {
      return "TaskParams{" +
          "delay=" + delay +
          ", period=" + period +
          ", timeUnit=" + timeUnit +
          '}';
    }
  }


}

