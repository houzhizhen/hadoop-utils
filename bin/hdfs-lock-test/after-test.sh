set -e
echo TAG=${TAG} in after-test.sh
TIME_SUFFIX=`date +"%H%M%S"`
curl http://xafj-sys-rpm23qygubp.xafj.baidu.com:8075/jmx > ${TAG}/${TIME_SUFFIX}-dn-rpm23qygubp.jmx 2>/dev/null
curl http://xafj-sys-rpm14usp6bo.xafj.baidu.com:8075/jmx > ${TAG}/${TIME_SUFFIX}-dn-rpm14usp6bo.jmx 2>/dev/null
curl http://xafj-sys-rpm723xv2lg.xafj.baidu.com:8075/jmx > ${TAG}/${TIME_SUFFIX}-dn-rpm723xv2lg.jmx 2>/dev/null
curl http://xafj-sys-rpm58y98bhi.xafj.baidu.com:8070/jmx > ${TAG}/${TIME_SUFFIX}-nn-rpm58y98bhi.jmx 2>/dev/null
grep WriteBlockOpAvgTime ${TAG}/${TIME_SUFFIX}*.jmx

if [ -n "${EXPECTED_WRITE_FILES}" ]; then
  echo "Validating WriteBlockOpNumOps >= ${EXPECTED_WRITE_FILES} on each DN..."
  for jmx_file in ${TAG}/${TIME_SUFFIX}-dn-*.jmx; do
    num_ops=$(grep -o '"WriteBlockOpNumOps" : [0-9]*' "$jmx_file" | awk '{print $NF}')
    if [ "$num_ops" -lt "$EXPECTED_WRITE_FILES" ]; then
      echo "ERROR: ${jmx_file} WriteBlockOpNumOps=${num_ops} < EXPECTED_WRITE_FILES=${EXPECTED_WRITE_FILES}"
      exit 1
    fi
    echo "OK: ${jmx_file} WriteBlockOpNumOps=${num_ops} >= ${EXPECTED_WRITE_FILES}"
  done
fi
