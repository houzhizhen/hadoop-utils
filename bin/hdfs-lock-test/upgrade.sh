#!/bin/bash
set -e
export PATH=/usr/local/bin:$PATH
COMMAND=$1
shift

SLAVES=$1
shift

echo command:$COMMAND
echo slaves:$SLAVES

#distribute files
if [ $COMMAND == "dist" ];then
  SRC=$1
  shift

  DEST=$1
  shift

  cat $SLAVES | while read slave
  do
    #已#?~@头?~Z~D注?~G~J?~O??~U??~G
    echo "$slave" | grep -q "^#"
    if [ $? -eq 0 ] ; then
        continue;
        fi

    echo "===================$slave================="
    echo scp -r  -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no $SRC $slave:$DEST
    scp -r  -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no $SRC $slave:$DEST
  done
  exit 0
fi

#common
if [ $COMMAND == "cmd" ];then
  cat $SLAVES | while read slave
  do
    #已#?~@头?~Z~D注?~G~J?~O??~U??~G
    echo "$slave" | grep -q "^#"
    if [ $? -eq 0 ] ; then
        continue;
    fi

    ssh  -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no $slave << EOF
      echo "==================$slave======================"
      eval $@
      exit
EOF
  done
  exit 0
fi

if [ $COMMAND == "passwd" ];then
  USER_NAME=$1
  if [ "${USER_NAME}" == "" ]; then
     echo There must be username.
     exit 1
  fi
  echo USER_NAME=${USER_NAME}
  cat $SLAVES | while read slave
  do
    #已#?~@头?~Z~D注?~G~J?~O??~U??~G
    echo "$slave" | grep -q "^#"
    if [ $? -eq 0 ] ; then
        continue;
    fi

    scp  -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no test $slave:/root >/dev/null

    ssh  -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no $slave << EOF
      echo "==================$slave======================"
      passwd  ${USER_NAME} --stdin < /root/test
      rm -f /root/test
      exit
EOF
  done
  rm -f test

  exit 0      
fi
