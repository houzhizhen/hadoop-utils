package org.apache.hadoop.hdfs.server.azmover;

public class SourceReadLimitException extends IgnoredException{

  public SourceReadLimitException(String message) {
    super(message);
  }
}

