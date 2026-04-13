set -e
sleep 10
echo TAG=${TAG} in after-test.sh
TIME_SUFFIX=`date +"%H%M%S"`
curl http://xafj-sys-rpm23qygubp.xafj.baidu.com:8075/jmx > ${TAG}/${TIME_SUFFIX}-dn-rpm23qygubp.jmx 2>/dev/null
curl http://xafj-sys-rpm14usp6bo.xafj.baidu.com:8075/jmx > ${TAG}/${TIME_SUFFIX}-dn-rpm14usp6bo.jmx 2>/dev/null
curl http://xafj-sys-rpm723xv2lg.xafj.baidu.com:8075/jmx > ${TAG}/${TIME_SUFFIX}-dn-rpm723xv2lg.jmx 2>/dev/null
curl http://xafj-sys-rpm58y98bhi.xafj.baidu.com:8070/jmx > ${TAG}/${TIME_SUFFIX}-nn-rpm58y98bhi.jmx 2>/dev/null
grep WriteBlockOpAvgTime ${TAG}/${TIME_SUFFIX}*.jmx
