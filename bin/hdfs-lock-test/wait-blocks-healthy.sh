set -e

# 等待 HDFS 所有数据块健康（无 Under-replicated、Missing、Corrupt 块）
# 通过 hdfs dfsadmin -report 获取汇总信息
# 用法: sh -x wait-blocks-healthy.sh [max-wait-seconds] [interval-seconds]
# 默认: max-wait=1800(30分钟)  interval=10

MAX_WAIT=${1:-1800}
INTERVAL=${2:-10}
WAITED=0

echo "等待 HDFS 数据块健康..."
echo "最大等待: ${MAX_WAIT}s"
echo "检查间隔: ${INTERVAL}s"

while true; do
  REPORT=$(hdfs dfsadmin -report 2>/dev/null || true)

  UNDER_REPLICATED=$(echo "${REPORT}" | grep 'Under replicated blocks:' | grep -oE '[0-9]+$' || echo "0")
  CORRUPT_BLOCKS=$(echo "${REPORT}" | grep 'Blocks with corrupt replicas:' | grep -oE '[0-9]+$' || echo "0")
  MISSING_BLOCKS=$(echo "${REPORT}" | grep 'Missing blocks:' | head -1 | grep -oE '[0-9]+$' || echo "0")
  MISSING_BLOCKS_RF1=$(echo "${REPORT}" | grep 'Missing blocks (with replication factor 1):' | grep -oE '[0-9]+$' || echo "0")

  echo "[${WAITED}s] UnderReplicated=${UNDER_REPLICATED} Corrupt=${CORRUPT_BLOCKS} Missing=${MISSING_BLOCKS} MissingRF1=${MISSING_BLOCKS_RF1}"

  if [ "${UNDER_REPLICATED}" -eq 0 ] && [ "${CORRUPT_BLOCKS}" -eq 0 ] && [ "${MISSING_BLOCKS}" -eq 0 ]; then
    echo "所有数据块健康! 等待 ${WAITED}s"
    break
  fi

  if [ "${WAITED}" -ge "${MAX_WAIT}" ]; then
    echo "ERROR: 等待 ${MAX_WAIT}s 后数据块仍未健康"
    echo "UnderReplicated=${UNDER_REPLICATED} Corrupt=${CORRUPT_BLOCKS} Missing=${MISSING_BLOCKS} MissingRF1=${MISSING_BLOCKS_RF1}"
    exit 1
  fi

  sleep "${INTERVAL}"
  WAITED=$((WAITED + INTERVAL))
done
