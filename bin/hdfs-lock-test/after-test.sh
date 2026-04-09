sleep 10
echo TAG=${TAG} in after-test.sh 
curl http://xafj-sys-rpm23qygubp.xafj.baidu.com:8075/jmx > ${TAG}/dn-rpm23qygubp.jmx
curl http://xafj-sys-rpm14usp6bo.xafj.baidu.com:8075/jmx > ${TAG}/dn-rpm14usp6bo.jmx
curl http://xafj-sys-rpm723xv2lg.xafj.baidu.com:8075/jmx > ${TAG}/dn-rpm723xv2lg.jmx
curl http://xafj-sys-rpm58y98bhi.xafj.baidu.com:8070/jmx >  ${TAG}/nn-rpm58y98bhi.jmx
grep WriteBlockOpAvgTime ${TAG}/*.jmx
