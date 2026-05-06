set -e
./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-hdd /etc/hadoop/conf/hdfs-site.xml
./upgrade.sh dist namenode /etc/hadoop/conf/hdfs-site.xml-hdd /etc/hadoop/conf/hdfs-site.xml
sh -x use_share_old.sh
sh -x dfsio-3-times.sh "dfsio-before-opt"

sh -x use_share_new.sh
sh -x dfsio-3-times.sh "dfsio-after-opt" 
