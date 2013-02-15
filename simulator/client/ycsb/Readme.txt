#!/bin/bash

java -cp ../lib/xmemcached.jar:../lib/spymemcached-2.7.3.jar:../lib/slf4j-api-1.6.4.jar:. com.yahoo.ycsb.experiment.Client -p recordcount=0 -p operationcount=0 -threads $1 -P prop_file.prop -f load.txt

