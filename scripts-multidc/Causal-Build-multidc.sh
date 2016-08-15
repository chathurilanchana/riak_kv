#!/bin/bash


 # rm -r riak_kv; \ copy this to cd deps
   # git clone -b Riak-Kv-Causal-dev https://github.com/chathurilanchana/riak_kv.git; \
#    wait; \


riak_dir=/root/Riak/riak

#set params
hb_freq=10
my_ip=riak@172.31.0.240,riak@172.31.0.245,riak@172.31.0.233,riak@172.31.0.229,riak@172.31.0.237,riak@172.31.0.238,riak@172.31.0.241,riak@172.31.0.242,riak@172.31.0.217
total_dcs=3
my_id=1
echo myip$my_ip

unset privateIps
while IFS= read -r line; do
    privateIps+=("$line")
done <  riaknodes1.config

wait
count=0
for i in ${privateIps[@]}; do
echo dc${clusters[count]}
echo inside$i
    (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    cd $riak_dir; \
    ulimit -n 65536; \
    rel/riak/bin/riak stop; \
    sleep 5; \
    rm -r rel/riak; \
    cd deps; \
    cd ..; \
    sleep 3; \
    make rel; \
    sleep 60; \
    ulimit -n 65536; \
    echo $riak_dir; \
    sed -i s/'127.0.0.1'/$i/g $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_hb_freq.*/ordering_service_hb_freq= $hb_freq/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_total_dcs.*/ordering_service_total_dcs= $total_dcs/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_my_dc_id.*/ordering_service_my_dc_id=$my_id/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/myip.*/myip= $my_ip/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ring_size.*/ring_size= 16/\" $riak_dir/rel/riak/etc/riak.conf; \
    rel/riak/bin/riak start; \
    sleep  10; \
    rel/riak/bin/riak-admin cluster join riak@${privateIps[0]}; \

" 2>&1 | awk '{ print "'$i': "$0 }' ) &
echo "before sleep"
sleep 60;
echo "woke-up"
 count=$((counter+1))
done

wait
echo "reach after cleanup"

  (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no  root@${privateIps[0]} "\
    cd $riak_dir; \
    rel/riak/bin/riak-admin ringready; \
    rel/riak/bin/riak-admin cluster plan; \
    rel/riak/bin/riak-admin cluster commit; \
" 2>&1 | awk '{ print "'$i': "$0 }' ) &

wait
echo "all executed"
