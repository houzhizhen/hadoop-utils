package com.baidu.hadoop.hdfs.blockmanager;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.SequentialBlockGroupIdGenerator;
import org.apache.hadoop.util.SequentialNumber;

public class TestBlockIdGenerator  extends SequentialNumber {
/**
 * Create a new instance with the given initial value.
 *
 * @param initialValue initialValue.
 */
protected TestBlockIdGenerator(long initialValue) {
    super(initialValue);
}

public static void main(String[] args) {
    TestBlockIdGenerator sequentialNumber = new TestBlockIdGenerator(Long.MIN_VALUE);
    long previousValue = Long.MIN_VALUE;
    for (int i = 0; i < 10; i++) {
        sequentialNumber.skipTo((sequentialNumber.getCurrentValue() & -16L) + 16L);
        long currentValue = sequentialNumber.getCurrentValue();
        System.out.println("diff " + (currentValue - previousValue));
        previousValue = currentValue;
    }


}
}
