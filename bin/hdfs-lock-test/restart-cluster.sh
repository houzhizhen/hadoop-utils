set -e
mapred --daemon stop historyserver
./upgrade.sh cmd namenode 'yarn --daemon stop resourcemanager'
./upgrade.sh cmd datanodes 'yarn --daemon stop nodemanager'
./upgrade.sh cmd namenode 'hdfs --daemon stop namenode'
./upgrade.sh cmd datanodes 'hdfs --daemon stop datanode'
./upgrade.sh cmd namenode 'jps'
./upgrade.sh cmd datanodes 'jps'
echo "Waiting for processes to fully stop..."
sleep 20
./upgrade.sh cmd namenode 'jps'
./upgrade.sh cmd datanodes 'jps'

echo starting hdfs
./upgrade.sh cmd namenode 'hdfs --daemon start namenode'
./upgrade.sh cmd datanodes 'hdfs --daemon start datanode'
mapred --daemon start historyserver
./upgrade.sh cmd namenode 'yarn --daemon start resourcemanager'
./upgrade.sh cmd datanodes 'yarn --daemon start nodemanager'
./upgrade.sh cmd namenode 'jps; ps aux | grep java'
./upgrade.sh cmd datanodes 'jps; ps aux | grep java'

sleep 120

# 检查 DataNode 数量
LIVE_DN=$(hdfs dfsadmin -report 2>/dev/null | grep '^Live datanodes' | grep -oE '[0-9]+')
echo "Live DataNodes: ${LIVE_DN}"
if [ "${LIVE_DN}" != "3" ]; then
  echo "ERROR: Expected 3 DataNodes but got ${LIVE_DN}"
  exit 1
fi

# 检查 NodeManager 数量
LIVE_NM=$(yarn node -list 2>/dev/null | grep '^Total Nodes' | grep -oE '[0-9]+')
echo "Live NodeManagers: ${LIVE_NM}"
if [ "${LIVE_NM}" != "3" ]; then
  echo "ERROR: Expected 3 NodeManagers but got ${LIVE_NM}"
  exit 1
fi
