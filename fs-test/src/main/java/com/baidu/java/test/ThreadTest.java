package com.baidu.java.test;

import java.util.concurrent.TimeUnit;

/**
 * 调用 线程.interrupt, 在线程中可能被忽略，线程继续执行。
 */
public class ThreadTest {

    private static class Log implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < 10; i++) {
                System.out.println("Thread: " + Thread.currentThread().getId() + " log " + i);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    // e.printStackTrace();
                    //  Thread.currentThread().interrupt();
                }
            }
        }
    }
    public static void main(String[] args) {

        System.out.println("new Ping");
        Thread ping = new Thread(new Log(),"ping thread");
        ping.setDaemon(true);
        ping.start();
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ping.interrupt();


        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            // e.printStackTrace();
        }
    }
}
