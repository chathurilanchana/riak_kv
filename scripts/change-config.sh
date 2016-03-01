#!/bin/bash

riak_dir=/root/Riak/riak
k=0
ring_size=128
hb_freq=1000

#put cleanips and private ips in relavant order

unset nodes
while IFS= read -r line; do 
    nodes+=("$line") 
done <  riaknodes.config

cmd "${args[@]}"

privateIps=("172.31.0.180")
privateJoinIp="172.31.0.180"

for i in ${privateIps[@]}; do
 privateIp=$(echo ${privateIps[$k]})
echo inside$i
    (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    cd $riak_dir; \
    rel/riak/bin/riak stop; \
    sleep 20; \
    rm -r data; \ 
    rm -r log; \ 
    sudo sed -i -e \"s/.*ring_size.*/ring_size= $ring_size/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_hb_freq.*/ordering_service_hb_freq= $hb_freq/\" $riak_dir/rel/riak/etc/riak.conf; \
    rel/riak/bin/riak start; \
    sleep 20; \
    rel/riak/bin/riak-admin test; \
    rel/riak/bin/riak-admin cluster join riak@$privateJoinIp; \
    sleep 10; \
" 2>&1 | awk '{ print "'$i': "$0 }' ) &
done
