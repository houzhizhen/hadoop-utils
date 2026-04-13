
set -e
./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-hdd /etc/hadoop/conf/hdfs-site.xml
sh -x use_share_old.sh
sh -x parallel-write-test.sh "parallel-hdd-before-opt-time1"
sh -x parallel-write-test.sh "parallel-hdd-before-opt-time2"
sh -x parallel-write-test.sh "parallel-hdd-before-opt-time3"
sh -x use_share_new.sh
sh -x parallel-write-test.sh "parallel-hdd-after-opt-time1"
sh -x parallel-write-test.sh "parallel-hdd-after-opt-time2"
sh -x parallel-write-test.sh "parallel-hdd-after-opt-time3"
echo parallel-test-hdd done
