set -e
export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020

sh -x pre-test.sh

export EXPECTED_WRITE_FILES=$((40 * 200 * 100))
hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedReadWriteByPercent \
 --maps 40 \
 --sleepTime 40000 \
 --baseDir ${TARGET_FS}/test/distributed_test \
 --parameters "--parallel 200 --readPercent 0 --filesize 10240 --fileNumPerThread 100"

sh -x after-test.sh
