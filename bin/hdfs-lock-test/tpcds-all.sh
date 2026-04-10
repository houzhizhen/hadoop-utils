set -e
sh -x use_share_old.sh
sh -x tpcds-3-times.sh "tpcds-before-opt"

sh -x use_share_new.sh
sh -x tpcds-3-times.sh "tpcds-after-opt"
