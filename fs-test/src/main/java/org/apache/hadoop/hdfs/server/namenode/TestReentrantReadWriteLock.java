package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestReentrantReadWriteLock {
public static void main(String[] args) {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    ReentrantReadWriteLock.WriteLock writeLock1 = readWriteLock.writeLock();
    ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    writeLock1.lock();
    writeLock.lock();
    System.out.println("readWriteLock.getWriteHoldCount():" + readWriteLock.getWriteHoldCount());
    System.out.println("readWriteLock.getReadHoldCount():" + readWriteLock.getReadHoldCount());

    writeLock1.unlock();
    writeLock.unlock();

}
}
