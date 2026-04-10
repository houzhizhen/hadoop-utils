set -e
export TAG=$1-nnbench1-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh -x nnbench.sh > ${TAG}/nnbench1.log 2>&1
export TAG=$1-nnbench2-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh -x nnbench.sh > ${TAG}/nnbench2.log 2>&1
export TAG=$1-nnbench3-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh -x nnbench.sh > ${TAG}/nnbench3.log 2>&1
