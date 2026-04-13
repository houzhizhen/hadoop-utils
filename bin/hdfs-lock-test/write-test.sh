echo begin 160 files `date `
hadoop jar /opt/bmr/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.6-tests.jar  TestDFSIO  -write -nrFiles 160 -size 20GB
echo end 160 files `date `
hadoop fs -rm -r hdfs://bjdd-sys-rpm27-7e2c1.bjdd.baidu.com:8020/*
sleep 300

echo begin 80 files `date `
hadoop jar /opt/bmr/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.6-tests.jar  TestDFSIO  -write -nrFiles 80 -size 20GB

echo end 80 files `date `
hadoop fs -rm -r hdfs://bjdd-sys-rpm27-7e2c1.bjdd.baidu.com:8020/*
sleep 300

echo begin 40 files `date `
hadoop jar /opt/bmr/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.6-tests.jar  TestDFSIO  -write -nrFiles 40 -size 20GB
echo end 40 files `date `
hadoop fs -rm -r hdfs://bjdd-sys-rpm27-7e2c1.bjdd.baidu.com:8020/*
sleep 300

echo begin 20 files `date `
hadoop jar /opt/bmr/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.6-tests.jar  TestDFSIO  -write -nrFiles 20 -size 20GB
echo end 20 files `date `
hadoop fs -rm -r hdfs://bjdd-sys-rpm27-7e2c1.bjdd.baidu.com:8020/*
sleep 300

echo begin 160 files `date `
hadoop jar /opt/bmr/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.6-tests.jar  TestDFSIO  -write -nrFiles 160 -size 20GB
echo end 160 files `date `
hadoop fs -rm -r hdfs://bjdd-sys-rpm27-7e2c1.bjdd.baidu.com:8020/*
sleep 300
