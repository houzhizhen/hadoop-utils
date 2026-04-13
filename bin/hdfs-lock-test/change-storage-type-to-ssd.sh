cd $(dirname $0);
echo CURRENT_DIR=`pwd`
./upgrade.sh dist datanodes /etc/hadoop/conf/hdfs-site.xml-ssd /etc/hadoop/conf/hdfs-site.xml
./upgrade.sh cmd datanodes 'grep -A 2 data.dir /etc/hadoop/conf/hdfs-site.xml'
