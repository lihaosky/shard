#!/bin/bash
nodes=`cat NodeList`
for word in $nodes
do
    ssh -f $word "killall mono; killall memcached"
done
