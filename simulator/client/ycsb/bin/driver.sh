echo `date +%s` >> time.txt 
java -cp ../lib/xmemcached.jar:../lib/spymemcached-2.7.3.jar:../lib/slf4j-api-1.6.4.jar:.  com.yahoo.ycsb.memcached.MemcachedClient -p recordcount=0 -p operationcount=$1 -threads $2 -P $3 -c $4 $5
echo `date +%s` >> time.txt
