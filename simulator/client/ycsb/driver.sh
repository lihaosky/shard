#!/bin/bash
if [ $# -ne 4 ]
then
    echo 'Usage: ./driver.sh nodenumber operationnumber threadnum [-false|controllerhost:controllerport]'
    exit
fi

nodenum=$1

nodes=`cat NodeList`
for word in $nodes
do
    words[$i]=$word
    ((i=$i+1))
done

#Empty hostlist
echo -n "" > hostlist.txt

#Write nodenum nodes into hostlist
i=0
while [ $i -lt $nodenum ]
do
    echo ${words[$i]}:9999 >> hostlist.txt
    ((i=$i+1))
done

java -cp ../lib/xmemcached.jar:../lib/spymemcached-2.7.3.jar:../lib/slf4j-api-1.6.4.jar:. com.yahoo.ycsb.memcached.MemcachedClient -p recordcount=0 -p operationcount=$2 -threads $3 $4 -P prop_file.prop -f load

