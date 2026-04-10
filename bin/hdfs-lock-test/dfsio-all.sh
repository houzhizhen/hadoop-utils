set -e

sh -x use_share_old.sh
sh -x dfsio-3-times.sh "dfsio-before-opt"

sh -x use_share_new.sh
sh -x dfsio-3-times.sh "dfsio-after-opt" 
