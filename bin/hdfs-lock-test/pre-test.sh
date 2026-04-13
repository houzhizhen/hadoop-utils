export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020/
## 删除 HDFS 文件
hadoop fs -rm -r -skipTrash ${TARGET_FS}/*
sleep 200
set -e
## 等待 numblocks 的数量是 0
curl http://xafj-sys-rpm14usp6bo.xafj:8075/jmx 2>/dev/null | grep VolumeInfo
curl http://xafj-sys-rpm23qygubp.xafj:8075/jmx 2>/dev/null | grep VolumeInfo
curl http://xafj-sys-rpm723xv2lg.xafj:8075/jmx 2>/dev/null | grep VolumeInfo
hdfs dfsadmin -safemode enter
hdfs dfsadmin -saveNamespace
# 重启 cluster，最多重试 10 次
for i in $(seq 1 10); do
  echo "=== restart-cluster.sh attempt $i/10 ==="
  if sh -x restart-cluster.sh; then
    echo "restart-cluster.sh succeeded on attempt $i"
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "ERROR: restart-cluster.sh failed after 10 attempts, exiting."
    exit 1
  fi
  echo "restart-cluster.sh failed on attempt $i, retrying..."
done
