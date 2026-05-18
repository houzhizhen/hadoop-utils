package com.baidu.fs.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 测试 MutableStat 的 snapshot 行为，验证 *.period 对 AvgTime 的影响。
 *
 * <p>核心结论：
 * <ul>
 *   <li>AvgTime 不是累积平均值，而是最近一个 snapshot 区间的平均值</li>
 *   <li>每次 snapshot() 后 intervalStat 被 reset</li>
 *   <li>如果 *.period 设置得足够大（如3600秒），测试期间不会触发 snapshot，
 *       测试结束后采集 JMX 时拿到的是整个测试的完整平均值</li>
 * </ul>
 */
public class TestMutableStatSnapshotPeriod {

    private static final double DELTA = 0.001;

    /**
     * 模拟默认 period=10s 的场景：测试过程中 snapshot 被触发，
     * 最终拿到的 AvgTime 只是最后一个窗口的值，不是全局平均。
     */
    @Test
    public void testSmallPeriodLosesGlobalAverage() {
        MutableStat stat = new MutableStat(
            "WriteBlockOp", "write block", "Ops", "Time");
        CapturingMetricsRecordBuilder mb;

        // 第一批数据：100次操作，每次耗时 200ms
        for (int i = 0; i < 100; i++) {
            stat.add(200);
        }

        // 模拟 period=10s 到期，触发第一次 snapshot
        mb = new CapturingMetricsRecordBuilder();
        stat.snapshot(mb, true);
        assertEquals("第一次 snapshot: numOps", 100, mb.capturedNumOps);
        assertEquals("第一次 snapshot: avgTime", 200.0, mb.capturedAvgTime, DELTA);

        // 第二批数据：100次操作，每次耗时 10ms（比如测试快要结束）
        for (int i = 0; i < 100; i++) {
            stat.add(10);
        }

        // 模拟 period=10s 再次到期，触发第二次 snapshot
        mb = new CapturingMetricsRecordBuilder();
        stat.snapshot(mb, true);
        // numOps 是累积的：200
        assertEquals("第二次 snapshot: numOps", 200, mb.capturedNumOps);
        // 但 avgTime 只是第二个窗口的平均值 10，而不是全局平均 (200*100+10*100)/200=105
        assertEquals("第二次 snapshot: avgTime 只反映最后一个窗口", 10.0, mb.capturedAvgTime, DELTA);
    }

    /**
     * 模拟 period=3600s 的场景：测试期间没有 snapshot 被触发，
     * 测试结束后采集 JMX 时拿到的是整个测试的完整平均值。
     */
    @Test
    public void testLargePeriodPreservesGlobalAverage() {
        MutableStat stat = new MutableStat(
            "WriteBlockOp", "write block", "Ops", "Time");

        // 与上面相同的两批数据，但不在中间触发 snapshot
        // 第一批：100次 200ms
        for (int i = 0; i < 100; i++) {
            stat.add(200);
        }
        // 第二批：100次 10ms
        for (int i = 0; i < 100; i++) {
            stat.add(10);
        }

        // 测试结束后才触发唯一一次 snapshot（模拟 JMX 查询）
        CapturingMetricsRecordBuilder mb = new CapturingMetricsRecordBuilder();
        stat.snapshot(mb, true);

        assertEquals("numOps 累积值", 200, mb.capturedNumOps);
        // avgTime 是全局平均：(200*100 + 10*100) / 200 = 105
        assertEquals("avgTime 是全局平均值", 105.0, mb.capturedAvgTime, DELTA);
    }

    /**
     * 验证 snapshot 之后无新数据时，AvgTime 保持 prevStat 不变。
     */
    @Test
    public void testNoDataAfterSnapshotReturnsPrevStat() {
        MutableStat stat = new MutableStat(
            "WriteBlockOp", "write block", "Ops", "Time");

        for (int i = 0; i < 50; i++) {
            stat.add(100);
        }

        // 第一次 snapshot
        CapturingMetricsRecordBuilder mb1 = new CapturingMetricsRecordBuilder();
        stat.snapshot(mb1, true);
        assertEquals(100.0, mb1.capturedAvgTime, DELTA);

        // 没有新数据，再次 snapshot
        CapturingMetricsRecordBuilder mb2 = new CapturingMetricsRecordBuilder();
        stat.snapshot(mb2, true);
        // AvgTime 仍然是上一个窗口的值
        assertEquals("无新数据时 avgTime 保持不变", 100.0, mb2.capturedAvgTime, DELTA);
        // numOps 不变
        assertEquals("无新数据时 numOps 保持不变", 50, mb2.capturedNumOps);
    }

    /**
     * 简单的 MetricsRecordBuilder 实现，用于捕获 snapshot 输出的 numOps 和 avgTime。
     */
    private static class CapturingMetricsRecordBuilder extends MetricsRecordBuilder {
        long capturedNumOps = -1;
        double capturedAvgTime = Double.NaN;
        private int counterCallCount = 0;
        private int gaugeCallCount = 0;

        @Override
        public MetricsRecordBuilder addCounter(org.apache.hadoop.metrics2.MetricsInfo info, long value) {
            // 第一个 counter 是 NumOps
            if (counterCallCount == 0) {
                capturedNumOps = value;
            }
            counterCallCount++;
            return this;
        }

        @Override
        public MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo info, double value) {
            // 第一个 gauge 是 AvgTime
            if (gaugeCallCount == 0) {
                capturedAvgTime = value;
            }
            gaugeCallCount++;
            return this;
        }


        // 以下为 MetricsRecordBuilder 抽象方法的空实现
        @Override public MetricsCollector parent() { return null; }
        @Override public MetricsRecordBuilder tag(org.apache.hadoop.metrics2.MetricsInfo info, String value) { return this; }
        @Override public MetricsRecordBuilder add(org.apache.hadoop.metrics2.MetricsTag tag) { return this; }
        @Override public MetricsRecordBuilder add(org.apache.hadoop.metrics2.AbstractMetric metric) { return this; }
        @Override public MetricsRecordBuilder setContext(String value) { return this; }
        @Override public MetricsRecordBuilder addCounter(org.apache.hadoop.metrics2.MetricsInfo info, int value) { return this; }
        @Override public MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo info, int value) { return this; }
        @Override public MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo info, long value) { return this; }
        @Override public MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo info, float value) { return this; }
    }
}
