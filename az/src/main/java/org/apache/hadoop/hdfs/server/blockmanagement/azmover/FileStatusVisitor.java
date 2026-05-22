package org.apache.hadoop.hdfs.server.azmover;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStatusVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(FileStatusVisitor.class);

  private final int threadNum;
  private final ThreadPoolExecutor pool;
  private final AtomicLong counter = new AtomicLong(0);
  private final AzMoverMetrics azMoverMetrics;

  public FileStatusVisitor(int threadNum) {
    this.threadNum = threadNum;
    this.pool = new ThreadPoolExecutor(threadNum, threadNum, 0L,
        TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(threadNum), (r, executor) -> {
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    this.azMoverMetrics = AzMoverMetrics.get(new Configuration());
  }

  public boolean isRunning() {
    return counter.get() != 0;
  }

  public void shutdown() {
    if (pool != null) {
      pool.shutdown();
    }
  }

  public void visit(
      DistributedFileSystem fileSystem,
      Path path,
      Collection<FileStatusFilter> filters,
      Consumer<HdfsLocatedFileStatus> fileConsumer,
      Consumer<Exception> exceptionHandler) {
    list(fileSystem, path, filters, fileConsumer, exceptionHandler);
  }

  public void visit(DistributedFileSystem fileSystem, Path path,
      Collection<FileStatusFilter> filters,
      Consumer<HdfsLocatedFileStatus> fileConsumer) {
    list(fileSystem,
        path,
        filters,
        fileConsumer,
        e -> LOG.warn(StringUtils.stringifyException(e)));
  }

  private void list(
      DistributedFileSystem fileSystem,
      Path path,
      Collection<FileStatusFilter> filters,
      Consumer<HdfsLocatedFileStatus> fileConsumer,
      Consumer<Exception> exceptionHandler) {
    counter.incrementAndGet();
    pool.execute(() -> {
      try {
        listInternal(fileSystem, path, filters, fileConsumer, exceptionHandler);
      } catch (Exception e) {
        exceptionHandler.accept(e);
      } finally {
        counter.decrementAndGet();
      }
    });
  }

  private void listInternal(
      DistributedFileSystem fileSystem,
      Path path,
      Collection<FileStatusFilter> filters,
      Consumer<HdfsLocatedFileStatus> fileConsumer,
      Consumer<Exception> exceptionHandler) throws IOException {
    RemoteIterator<LocatedFileStatus> iterator;
    try {
      iterator = fileSystem.listLocatedStatus(path);
    } catch (FileNotFoundException e) {
      LOG.warn("Path: {} does not exists, skip", path);
      return;
    }
    while (iterator.hasNext()) {
      azMoverMetrics.transferPathsChange(null, AzMoverMetrics.VISITED);
      LocatedFileStatus locatedFileStatus = iterator.next();
      if (!isAccepted(locatedFileStatus, filters)) {
        LOG.info("Skip path: {} by filters", locatedFileStatus.getPath());
        continue;
      }
      if (locatedFileStatus.isFile()) {
        fileConsumer.accept((HdfsLocatedFileStatus) locatedFileStatus);
      } else {
        Path child = locatedFileStatus.getPath();
        if (counter.get() >= threadNum) {
          listInternal(fileSystem, child, filters, fileConsumer, exceptionHandler);
        } else {
          list(fileSystem, child, filters, fileConsumer, exceptionHandler);
        }
      }
    }
  }

  public static boolean isAccepted(LocatedFileStatus fileStatus,
      Collection<FileStatusFilter> filters) {
    return filters.stream().allMatch(fileStatusFilter -> fileStatusFilter.isAccepted(fileStatus));
  }

  public interface FileStatusFilter {

    boolean isAccepted(LocatedFileStatus fileStatus);

    default String description() {
      return "";
    }
  }
}

