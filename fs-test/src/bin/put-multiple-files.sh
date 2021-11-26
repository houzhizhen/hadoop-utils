#!/usr/bin/env bash
## Open multiple files simultaneously. write specified size of data to each file, and then close them.
LOGFILE=put-multiple-files-`date +"%Y%m%d-%H%M%S"`.log
# parameters:  basePath threadNum subdirNum fileNum iterationTimes
hadoop jar ./fs-test-1.8.10.jar \
  com.baidu.fs.parallel.PutMultipleFiles \
  hdfs://localhost:9000/user/hive/warehouse/ \
  10 2100 > ${LOGFILE} 2>&1
