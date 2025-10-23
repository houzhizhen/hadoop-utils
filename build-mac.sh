mvn -T 1C  clean package\
 -DskipTests -Dos.arch=x86_64 \
 -B
#mvn -T 1C clean package -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip=true -Dhadoop.version=3.2.3 -Dguava.version=27.0-jre -B