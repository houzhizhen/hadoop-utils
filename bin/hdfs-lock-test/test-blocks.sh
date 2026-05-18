MAX_WAIT=600
WAITED=0
while true; do
  ALL_ZERO=true
    TOTAL_BLOCKS=$(cat parallel-hdd/parallel-hdd-before-opt-time1-20260413-101828/102602-dn-rpm14usp6bo.jmx | grep VolumeInfo | sed 's/\\"/"/g' | grep -o '"numBlocks":[0-9]*' | grep -o '[0-9]*' |  awk  '{s+=$1} END {print s+0}')
    if [ "$TOTAL_BLOCKS" -ne 0 ]; then
      ALL_ZERO=false
      echo "Waiting for blocks to be deleted... $url numBlocks=$TOTAL_BLOCKS (waited ${WAITED}s)"
      break
    fi
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