package com.baidu.java.test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestReentrantReadWriteLock {

public static void main(String[] args) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    boolean locked = lock.writeLock().tryLock();
    System.out.println(locked);
    System.out.println("locked = " + locked);
    locked = lock.writeLock().tryLock();
    System.out.println("locked = " + locked);
    System.out.println("Math.ceil(1.1): " + Math.ceil(1.1));
    System.out.println("writeLock.holdCount" + lock.writeLock().getHoldCount());

}
}
