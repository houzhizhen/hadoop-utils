#!/bin/bash
set -e

URL="http://xafj-sys-rpm14usp6bo.xafj.baidu.com:8075/jmx"

for i in $(seq 1 40); do
    curl -s "$URL" > "${i}.log"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - saved ${i}.log"
    if [ "$i" -lt 40 ]; then
        sleep 1
    fi
done
