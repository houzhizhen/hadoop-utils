LOGFILE=list-`date +"%Y%m%d-%H%M%S"`.log
hadoop jar target/fs-test-1.8.10.jar com.baidu.fs.parallel.List \
           2 hdfs://localhost:9000/user/hive/warehouse/ 10 10 \
           > ${LOGFILE} 2 > &1
