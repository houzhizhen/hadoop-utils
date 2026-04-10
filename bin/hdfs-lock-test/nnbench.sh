set -e
sh -x pre-test.sh
BASE_DIR=/benchmarks/NNBench
MAPS=40

# 执行 create（核心压测点）
hadoop jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar nnbench \
  -operation create_write \
  -baseDir $BASE_DIR \
  -maps $MAPS \
  -reduces 1 \
  -blockSize 1 \
  -bytesToWrite 0 \
  -numberOfFiles 1000 \
  -replicationFactorPerFile 1

sh -x after-test.sh 
