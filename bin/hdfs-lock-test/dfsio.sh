set -e

sh -x pre-test.sh
hadoop jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar TestDFSIO \
  -Dmapreduce.map.memory.mb=1024 \
  -Dmapreduce.map.java.opts=-Xmx700m \
  -write \
  -nrFiles 240 \
  -fileSize 4GB

sh -x after-test.sh
