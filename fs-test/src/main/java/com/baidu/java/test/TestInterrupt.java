package com.baidu.java.test;

import com.baidu.fs.distributed.DistributedReadTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestInterrupt {
private static final Logger LOG = LoggerFactory.getLogger(TestInterrupt.class);

    public static void main(String[] args) {
        InterruptAt interruptAt = new InterruptAt(Thread.currentThread(), 2000L);
        interruptAt.start();
        LOG.info("Begin do something");
        long doTime = 3000;
        long end = System.currentTimeMillis() + doTime;
        Random rand = new Random();
        while (System.currentTimeMillis() < end) {
            rand.nextDouble();
        }
        LOG.info("Finished do something");
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            LOG.info("Thread.currentThread().interrupt()");
            // Thread.currentThread().interrupt();
        }
        if (Thread.currentThread().isInterrupted()) {
            LOG.info("Thread.currentThread().isInterrupted()");
        }
        if (Thread.currentThread().isInterrupted()) {
            LOG.info("Thread.currentThread().isInterrupted()2");
        }

    }
static class InterruptAt extends Thread {
    Thread t;
    long time;
    InterruptAt(Thread t, long time) {
        this.t = t;
        this.time = time;
    }
    public void run() {
        long end = System.currentTimeMillis() + time;
        Random rand = new Random();
        while (System.currentTimeMillis() < end) {
            rand.nextDouble();
        }
        t.interrupt();
        TestInterrupt.LOG.info("send interrupt signal");
    }
}
}

