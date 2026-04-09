export TAG=$1-dfsio1-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh dfsio.sh > ${TAG}/dfsio1.log 2>&1
export TAG=$1-dfsio2-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh dfsio.sh > ${TAG}/dfsio2.log 2>&1
export TAG=$1-dfsio3-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh dfsio.sh > ${TAG}/dfsio3.log 2>&1
