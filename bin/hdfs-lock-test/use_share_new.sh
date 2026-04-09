cd $(dirname $0);
echo CURRENT_DIR=`pwd`
rm -rf /ssd1/hadoop-3.3.6/share; cp -r /ssd1/hadoop-3.3.6/share-new /ssd1/hadoop-3.3.6/share
./upgrade.sh cmd datanodes 'rm -rf /ssd1/hadoop-3.3.6/share; cp -r /ssd1/hadoop-3.3.6/share-new /ssd1/hadoop-3.3.6/share'
./upgrade.sh cmd namenode 'rm -rf /ssd1/hadoop-3.3.6/share; cp -r /ssd1/hadoop-3.3.6/share-new /ssd1/hadoop-3.3.6/share'

./upgrade.sh cmd datanodes 'du -s -h /ssd1/hadoop-3.3.6/share'
./upgrade.sh cmd namenode 'du -s -h /ssd1/hadoop-3.3.6/share'
