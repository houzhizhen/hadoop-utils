sh wait-blocks-healthy.sh 36000
curl http://xafj-sys-rpm58y98bhi.xafj.baidu.com:8070/jmx > nn-jmx-before-opt.xml
./upgrade.sh cmd third-node  'hdfs --daemon stop datanode'

export TAG="tpcds-before-opt"
sh tpcds-run.sh 
