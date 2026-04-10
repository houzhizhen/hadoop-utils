set -e
sleep 10
echo TAG=${TAG} in after-test.sh
TIME_SUFFIX=`date +"%H%M%S"`
curl http://xafj-sys-rpm23qygubp.xafj.baidu.com:8075/jmx > ${TAG}/dn-rpm23qygubp-${TIME_SUFFIX}.jmx 2>/dev/null
curl http://xafj-sys-rpm14usp6bo.xafj.baidu.com:8075/jmx > ${TAG}/dn-rpm14usp6bo-${TIME_SUFFIX}.jmx 2>/dev/null
curl http://xafj-sys-rpm723xv2lg.xafj.baidu.com:8075/jmx > ${TAG}/dn-rpm723xv2lg-${TIME_SUFFIX}.jmx 2>/dev/null
curl http://xafj-sys-rpm58y98bhi.xafj.baidu.com:8070/jmx > ${TAG}/nn-rpm58y98bhi-${TIME_SUFFIX}.jmx 2>/dev/null
grep WriteBlockOpAvgTime ${TAG}/*.jmx
