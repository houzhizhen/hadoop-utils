set -e
echo "test use_share_old.sh begin "
sh -x use_share_old.sh
echo "test use_share_old.sh end "
echo "test nnbench-3-times.sh begin "
sh -x nnbench-3-times.sh "nnbench-before-opt"
echo "test nnbench-3-times.sh end "
echo "test use_share_new.sh begin "
sh -x use_share_new.sh
echo "test use_share_new.sh end "
echo "test nnbench-after-opt begin "
sh -x nnbench-3-times.sh "nnbench-after-opt" 
echo "test nnbench-after-opt end "
