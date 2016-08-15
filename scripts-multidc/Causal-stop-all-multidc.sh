#!/bin/bash

riak_dir=/root/Riak/riak
k=0

# sudo sed -i -e \"s/.*ring_size.*/ring_size= $ring_size/\" $riak_dir/rel/riak/etc/riak.conf; \
#    sudo sed -i -e \"s/ordering_service_hb_freq.*/ordering_service_hb_freq= $hb_freq/\" $riak_dir/rel/riak/etc/riak.conf; \


#put cleanips and private ips in relavant order

unset privateIps
while IFS= read -r line; do 
    privateIps+=("$line") 
done <  riaknodes3.config

for i in ${privateIps[@]}; do
 echo inside$i
    (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    cd $riak_dir; \
    rel/riak/bin/riak stop; \
" 2>&1 | awk '{ print "'$i': "$0 }' ) &
done
wait
echo "all executed"
                    


