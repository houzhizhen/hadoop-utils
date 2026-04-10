set -e
sh -x use_share_old.sh
sh -x nnbench-3-times.sh "nnbench-before-opt"
sh -x use_share_new.sh
sh -x nnbench-3-times.sh "nnbench-after-opt" 
