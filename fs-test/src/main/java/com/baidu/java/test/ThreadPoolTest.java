package com.baidu.java.test;

import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ThreadPoolTest {
    static class R implements Runnable {

        private final int value;
        public R(int i) {
            this.value = i;
        }
        @Override
        public void run() {
            System.out.println("Run R " + value);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (value % 3 == 0) {
                throw new RuntimeException();
            }
        }
    }
    public static void main(String[] args) {

         ExecutorService es = BlockingThreadPoolExecutorService.newInstance(
                10,
                20,
                60, TimeUnit.SECONDS,
                "test-pool");

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<Future> futureList = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            futureList.add(es.submit(new R(i)));
        }

        for (int i = 0; i < 100; i++) {
            Future future = futureList.get(i);
            try {
                future.get(10, TimeUnit.SECONDS);
                System.out.println("get result " + i);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                System.out.println("failed get R " + i);
            }
        }
        es.shutdown();
    }
}
