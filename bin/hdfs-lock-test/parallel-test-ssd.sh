
set -e
./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-ssd /etc/hadoop/conf/hdfs-site.xml
sh -x use_share_old.sh
sh -x parallel-write-test.sh "parallel-ssd-before-opt-time1"
sh -x parallel-write-test.sh "parallel-ssd-before-opt-time2"
sh -x parallel-write-test.sh "parallel-ssd-before-opt-time3"
sh -x use_share_new.sh
sh -x parallel-write-test.sh "parallel-ssd-after-opt-time1"
sh -x parallel-write-test.sh "parallel-ssd-after-opt-time2"
sh -x parallel-write-test.sh "parallel-ssd-after-opt-time3"
