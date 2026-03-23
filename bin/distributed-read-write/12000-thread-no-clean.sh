export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020

# 检查是否传递了baseDir参数
if [ $# -eq 0 ]; then
    # 如果没有参数，使用默认值
    BASE_DIR="${TARGET_FS}/test/distributed_test"
else
    # 如果传递了参数，使用参数值
    BASE_DIR="${TARGET_FS}/test/$1"
fi

echo "Using base directory: $BASE_DIR"

hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedReadWriteByPercent \
 --maps 40 \
 --sleepTime 40000 \
 --baseDir $BASE_DIR \
 --parameters "--parallel 300 --readPercent 0  --filesize 10240 --fileNumPerThread 100"
