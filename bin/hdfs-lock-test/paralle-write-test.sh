set -e

export TAG=$1-`date +"%Y%m%d-%H%M%S"`
mkdir -p ${TAG}
sh 1000-thread.sh > ${TAG}/${TAG}-1000-thread.log 2>&1
sh 2000-thread.sh > ${TAG}/${TAG}-2000-thread.log 2>&1
sh 4000-thread.sh > ${TAG}/${TAG}-4000-thread.log 2>&1
sh 8000-thread.sh > ${TAG}/${TAG}-8000-thread.log 2>&1
sh 12000-thread.sh > ${TAG}/${TAG}-12000-thread.log 2>&1
