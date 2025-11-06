#!/bin/bash

cd $(dirname $0);
export MONITOR_LOG_DIR=${MONITOR_LOG_DIR:-"logs"}

if [ ! -d ${MONITOR_LOG_DIR} ]; then
  mkdir -p ${MONITOR_LOG_DIR}
fi

## delete obsolete logs
DELETE_LOGFILE=`date -d "-30 day" +"%Y%m%d"`
rm -rf ${MONITOR_LOG_DIR}/${DELETE_LOGFILE}*

export NAMENODE_HTTP_ADDR='bjdd-sys-rpm27-7e2c1.bjdd.baidu.com:8070'
export DATANODE_HTTP_ADDR='bjdd-acg-tge57tc7cnh.bjdd.baidu.com:8075'
export NAMENODE_CONF=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-namenode-conf.xml

curl ${NAMENODE_HTTP_ADDR}/conf > ${NAMENODE_CONF}

export NAMENODE_STACKS=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-namenode-stacks.xml

curl ${NAMENODE_HTTP_ADDR}:8070/stacks > ${NAMENODE_STACKS}

export NAMENODE_JMX=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-namenode-jmx.xml

curl ${NAMENODE_HTTP_ADDR}/jmx > ${NAMENODE_JMX}
## DATANODE
export DATANODE_CONF=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-datanode-conf.xml

curl ${DATANODE_HTTP_ADDR}/conf > ${DATANODE_CONF}

export DATANODE_STACKS=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-datanode-stacks.xml

curl ${DATANODE_HTTP_ADDR}/stacks > ${DATANODE_STACKS}
export DATANODE_JMX=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-datanode-jmx.xml

curl ${DATANODE_HTTP_ADDR}/jmx > ${DATANODE_JMX}
