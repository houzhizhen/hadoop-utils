## ConnectWithKerberosTest
Test the performance of connection to Namenode.

To compile the java.
cp ConnectWithKerberosTest.java to the Server. May content directory since it works with only one file.

export CLASSPATH=`hadoop classpath`
javac ConnectWithKerberosTest.java
if [ -f conn.jar ]; then
   rm conn.jar
fi
jar -cf conn.jar ./*
hadoop jar conn.jar ConnectWithKerberosTest ${PRINCIPLE} ${KEYTAB_FILE_LOCATION}
