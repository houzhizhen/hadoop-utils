set -e
export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020

sh -x tpcds-pre-test.sh

spark-submit --master yarn --deploy-mode client \
  --num-executors 30 --executor-cores 8 --executor-memory 14G \
  --jars ${SPARK_HOME}/jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  --class org.apache.spark.tpcdsTest.PerfTestTpcds \
  spark-tpcds-tools/TPCDSTest.jar \
  --rootDir ${TARGET_FS}/tpcds_data_5t_parquet \
  --resultLocation ${TARGET_FS}/tpcds_result/${TAG} \
  --databaseName tpcds_5t \
  --scaleFactor 5000 --queries all --iterations 1

sh -x after-test.sh
