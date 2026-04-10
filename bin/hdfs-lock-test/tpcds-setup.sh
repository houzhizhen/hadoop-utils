set -e
export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020

# 分发 tpcds-kit 到所有节点
unzip spark-tpcds-tools/tpcds-kit.zip -d /opt/
./upgrade.sh dist datanodes /opt/tpcds-kit /opt/
./upgrade.sh dist namenode /opt/tpcds-kit /opt/

# 拷贝 spark-sql-perf jar 到 Spark jars 目录
cp spark-tpcds-tools/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar ${SPARK_HOME}/jars/

# 生成 5TB parquet 数据到 HDFS
spark-submit --master yarn --deploy-mode cluster \
  --num-executors 30 --executor-cores 8 --executor-memory 14G \
  --conf spark.sql.shuffle.partitions=256 \
  --jars ${SPARK_HOME}/jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  --class org.apache.spark.tentgendata.Tentgendata \
  spark-tpcds-tools/TPCDSTest.jar \
  --rootDir ${TARGET_FS}/tpcds_data_5t_parquet \
  --scaleFactor 5000 --format parquet \
  --dsdgenDir /opt/tpcds-kit/tools --numPartitions 256

# 创建外表
spark-submit --master yarn --deploy-mode client \
  --num-executors 30 --executor-cores 8 --executor-memory 14G \
  --jars ${SPARK_HOME}/jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  --class org.apache.spark.createExternalTable.CreateExtTable \
  spark-tpcds-tools/TPCDSTest.jar \
  --rootDir ${TARGET_FS}/tpcds_data_5t_parquet \
  --scaleFactor 5000 --format parquet --database tpcds_5t
