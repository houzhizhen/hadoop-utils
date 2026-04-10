set -e

./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-hdd /etc/hadoop/conf/hdfs-site.xml
sh use_share_old.sh
sh paralle-write-test.sh "parallel-hdd-before-opt"

sh use_share_new.sh
sh paralle-write-test.sh "parallel-hdd-after-opt" 

./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-ssd /etc/hadoop/conf/hdfs-site.xml

sh use_share_old.sh
sh paralle-write-test.sh "parallel-ssd-before-opt"

sh use_share_new.sh
sh paralle-write-test.sh "parallel-ssd-after-opt" 
## restore hdd
./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-hdd /etc/hadoop/conf/hdfs-site.xml
