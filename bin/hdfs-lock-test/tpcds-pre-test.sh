set -e
# TPC-DS 测试前重启集群清缓存（不删除 HDFS 数据）
sh -x restart-cluster.sh
sleep 50
