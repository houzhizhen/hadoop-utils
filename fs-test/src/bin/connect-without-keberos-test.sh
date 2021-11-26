#!/usr/bin/env bash

export TIMES=$1
export LIST=$2

export CLASSPATH=`hadoop classpath`
javac ConnectWithoutKerberosTest.java
if [ -f conn.jar ]; then
   rm conn.jar
fi
jar -cf conn.jar ./*
hadoop jar conn.jar ConnectWithoutKerberosTest ${TIMES} ${LIST}
