export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020
## 删除 HDFS 文件
hadoop fs -rm -r -skipTrash ${TARGET_FS}/*
sleep 200
## 等待 numblocks 的数量是 0
curl http://xafj-sys-rpm14usp6bo.xafj:8075/jmx | grep VolumeInfo
curl xafj-sys-rpm23qygubp.xafj:8075/jmx | grep VolumeInfo
## 重启 datanode
./upgrade.sh cmd datanodes 'hdfs --daemon stop datanode; hdfs --daemon start datanode'
./upgrade.sh cmd datanodes 'ps aux | grep DataNode'
sleep 20