package org.apache.hadoop.hdfs.server.azmover;

public class IgnoredException extends RuntimeException{

  public IgnoredException(String message) {
    super(message);
  }
}

