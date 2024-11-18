LOGFILE=long-time-dir-test-`date +"%Y%m%d-%H%M%S"`.log
nohup hadoop jar fs-test-1.8.10.jar com.baidu.fs.test.LongTimeDirTest \
           bos://bmr-rd-wh/long-time-test 1000 \
           > ${LOGFILE} 2>&1 &