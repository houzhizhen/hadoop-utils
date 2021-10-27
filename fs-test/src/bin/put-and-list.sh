LOGFILE=put-and-list-`date +"%Y%m%d-%H%M%S"`.log
# parameters:  basePath threadNum subdirNum fileNum iterationTimes
hadoop jar target/fs-test-1.8.10.jar \
  com.baidu.fs.parallel.PutAndList \
  hdfs://localhost:9000/user/hive/warehouse/ \
  2 10 20 30 > ${LOGFILE} 2>&1
