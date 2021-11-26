#!/usr/bin/env bash

export PRINCIPLE=$1
export KEYTAB_FILE_LOCATION=$2
export TIMES=$3

export CLASSPATH=`hadoop classpath`
javac ConnectWithKerberosTest.java
if [ -f conn.jar ]; then
   rm conn.jar
fi
jar -cf conn.jar ./*
hadoop jar conn.jar ConnectWithKerberosTest ${PRINCIPLE} ${KEYTAB_FILE_LOCATION} ${TIMES}
