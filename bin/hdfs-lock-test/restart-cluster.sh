mapred --daemon stop historyserver
./upgrade.sh cmd namenode 'yarn --daemon stop resourcemanager'
./upgrade.sh cmd datanodes 'yarn --daemon stop nodemanager'
./upgrade.sh cmd namenode 'hdfs --daemon stop namenode'
./upgrade.sh cmd datanodes 'hdfs --daemon stop datanode'
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
