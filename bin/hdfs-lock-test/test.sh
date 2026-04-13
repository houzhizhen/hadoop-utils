./upgrade.sh cmd nodemanagers 'hdfs --daemon start datanode'
sleep 60
sh write-test.sh > write-20-disk.log 2>&1
./upgrade.sh cmd nodemanagers 'hdfs --daemon stop datanode'
sleep 1000
sh read-test.sh > read-20-disk.log 2>&1
