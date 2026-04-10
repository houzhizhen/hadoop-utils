set -e
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
sh -x restart-cluster.sh
sleep 120
# 检查 DataNode 数量
LIVE_DN=$(hdfs dfsadmin -report 2>/dev/null | grep '^Live datanodes' | grep -oE '[0-9]+')
echo "Live DataNodes: ${LIVE_DN}"
if [ "${LIVE_DN}" != "3" ]; then
  echo "ERROR: Expected 3 DataNodes but got ${LIVE_DN}, exiting."
  exit 1
fi

# 检查 NodeManager 数量
LIVE_NM=$(yarn node -list 2>/dev/null | grep '^Total Nodes' | grep -oE '[0-9]+')
echo "Live NodeManagers: ${LIVE_NM}"
if [ "${LIVE_NM}" != "3" ]; then
  echo "ERROR: Expected 3 NodeManagers but got ${LIVE_NM}, exiting."
  exit 1
fi
