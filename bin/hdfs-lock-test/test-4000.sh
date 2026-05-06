export TAG=hdd-`date +"%Y%m%d-%H%M%S"`
mkdir ${TAG}
sh -x 4000-thread.sh > ${TAG}/${TAG}-4000-thread.log 2>&1 
