export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020
hadoop jar fs-test-1.8.10.jar com.baidu.fs.raw.RepeatCreateSameFile \
  --hdfs-path ${TARGET_FS}/tmp/test.txt \
  --size 10KB \
  --time 20m 

