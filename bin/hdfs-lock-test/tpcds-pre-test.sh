set -e
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
