find src/ -name *.java > source.txt
if [ ! -d "bin" ]
then
mkdir bin/
fi
javac @source.txt -cp lib/xmemcached.jar:lib/spymemcached-2.7.3.jar:lib/slf4j-api-1.6.4.jar:lib/jackson-core-asl-1.5.2.jar:lib/jackson-mapper-asl-1.5.2.jar:lib/java_memcached-release_2.0.1.jar:lib/hibernate-3.2.0.cr2.jar:lib/hibernate-memcached.jar:lib/log4j-over-slf4j-1.6.4.jar:lib/org.springframework.beans-3.1.1.RELEASE.jar:lib/slf4j-log4j12-1.6.4.jar:lib/slf4j-simple-1.6.4.jar:lib/slf4j-simple-1.6.4.jar:lib -d bin/
