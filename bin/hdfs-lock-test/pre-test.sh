export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020
## 删除 HDFS 文件
hadoop fs -rm -r -skipTrash ${TARGET_FS}/*
sleep 200
## 等待 numblocks 的数量是 0
curl http://xafj-sys-rpm14usp6bo.xafj:8075/jmx 2>/dev/null | grep VolumeInfo
curl http://xafj-sys-rpm23qygubp.xafj:8075/jmx 2>/dev/null | grep VolumeInfo
curl http://xafj-sys-rpm723xv2lg.xafj:8075/jmx 2>/dev/null | grep VolumeInfo
hdfs dfsadmin -safemode enter
hdfs dfsadmin -saveNamespace
# 重启 cluster
sh restart-cluster.sh
sleep 50
