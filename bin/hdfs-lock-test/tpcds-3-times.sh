set -e
export TAG=$1-tpcds1-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh -x tpcds-run.sh > ${TAG}/tpcds1.log 2>&1
export TAG=$1-tpcds2-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh -x tpcds-run.sh > ${TAG}/tpcds2.log 2>&1
export TAG=$1-tpcds3-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh -x tpcds-run.sh > ${TAG}/tpcds3.log 2>&1
