# 分发 tpcds-kit 到所有节点
unzip spark-tpcds-tools/tpcds-kit.zip -d /opt/
./upgrade.sh dist datanodes /opt/tpcds-kit /opt/
./upgrade.sh dist namenode /opt/tpcds-kit /opt/

# 拷贝 spark-sql-perf jar 到 Spark jars 目录
cp spark-tpcds-tools/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar ${SPARK_HOME}/jars/
