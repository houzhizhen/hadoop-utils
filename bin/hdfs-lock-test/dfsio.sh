sh pre-test.sh
hadoop jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar TestDFSIO \
  -write \
  -nrFiles 1000 \
  -fileSize 32MB

sh after-test.sh
