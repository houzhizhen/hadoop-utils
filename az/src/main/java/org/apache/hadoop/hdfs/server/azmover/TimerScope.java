package org.apache.hadoop.hdfs.server.azmover;

import java.io.Closeable;
import java.util.function.Consumer;

public class TimerScope implements Closeable {

  private final long start;
  private final boolean useNano;
  private final Consumer<Long> timeConsumer;

  public TimerScope() {
    this(time -> {
    }, false);
  }

  public TimerScope(Consumer<Long> timeConsumer) {
    this(timeConsumer, false);
  }

  public TimerScope(Consumer<Long> timeConsumer, boolean useNano) {
    this.timeConsumer = timeConsumer;
    this.useNano = useNano;
    if (useNano) {
      this.start = System.nanoTime();
    } else {
      this.start = System.currentTimeMillis();
    }
  }


  @Override
  public void close() {
    if (useNano) {
      timeConsumer.accept(System.nanoTime() - start);
    } else {
      timeConsumer.accept(System.currentTimeMillis() - start);
    }
  }
}

