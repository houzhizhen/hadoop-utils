export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020/
## 删除 HDFS 文件
hadoop fs -rm -r -skipTrash ${TARGET_FS}/*
set -e
sleep 10


hdfs dfsadmin -triggerBlockReport xafj-sys-rpm14usp6bo.xafj:8010
hdfs dfsadmin -triggerBlockReport xafj-sys-rpm23qygubp.xafj:8010
hdfs dfsadmin -triggerBlockReport xafj-sys-rpm723xv2lg.xafj:8010
## 等待所有 DataNode 的 numBlocks 归零
DN_URLS="http://xafj-sys-rpm14usp6bo.xafj.baidu.com:8075/jmx http://xafj-sys-rpm23qygubp.xafj.baidu.com:8075/jmx http://xafj-sys-rpm723xv2lg.xafj.baidu.com:8075/jmx"
MAX_WAIT=600
WAITED=0
while true; do
  ALL_ZERO=true
  for url in $DN_URLS; do
    TOTAL_BLOCKS=$(curl -s "$url" | grep VolumeInfo | sed 's/\\"/"/g' | grep -o '"numBlocks":[0-9]*' | grep -o '[0-9]*' | awk '{s+=$1} END {print s+0}')
    if [ "$TOTAL_BLOCKS" -ne 0 ]; then
      ALL_ZERO=false
      echo "Waiting for blocks to be deleted... $url numBlocks=$TOTAL_BLOCKS (waited ${WAITED}s)"
      break
    fi
  done
  if $ALL_ZERO; then
    echo "All DataNode numBlocks are 0, waited ${WAITED}s"
    break
  fi
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "ERROR: numBlocks not zero after ${MAX_WAIT}s"
    for url in $DN_URLS; do
      curl -s "$url" | grep VolumeInfo
    done
    exit 1
  fi
  sleep 10
  WAITED=$((WAITED + 10))
done
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
