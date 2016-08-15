#!/bin/bash

riak_dir=/root/Riak/riak
k=0

# sudo sed -i -e \"s/.*ring_size.*/ring_size= $ring_size/\" $riak_dir/rel/riak/etc/riak.conf; \
#    sudo sed -i -e \"s/ordering_service_hb_freq.*/ordering_service_hb_freq= $hb_freq/\" $riak_dir/rel/riak/etc/riak.conf; \


#put cleanips and private ips in relavant order
hb_freq=10
my_ip=riak@172.31.0.240,riak@172.31.0.245,riak@172.31.0.233,riak@172.31.0.229,riak@172.31.0.237,riak@172.31.0.238,riak@172.31.0.241,riak@172.31.0.242,riak@172.31.0.217
total_dcs=3
my_id=3
gst_timer_freq=50

unset privateIps
while IFS= read -r line; do 
    privateIps+=("$line") 
done <  riaknodes3.config

for i in ${privateIps[@]}; do
 echo inside$i
    (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    cd $riak_dir; \
    rel/riak/bin/riak stop; \
    sleep 20; \
    rm -r rel/riak/log; \ 
    ulimit -n 65536; \
  sed -i s/'127.0.0.1'/$i/g $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_hb_freq.*/ordering_service_hb_freq= $hb_freq/\" $riak_dir/rel/riak/etc/riak.conf; \
      sudo sed -i -e \"s/gst_timer_freq.*/gst_timer_freq= $gst_timer_freq/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_total_dcs.*/ordering_service_total_dcs= $total_dcs/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ordering_service_my_dc_id.*/ordering_service_my_dc_id=$my_id/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/myip.*/myip= $my_ip/\" $riak_dir/rel/riak/etc/riak.conf; \
    sudo sed -i -e \"s/ring_size.*/ring_size= 8/\" $riak_dir/rel/riak/etc/riak.conf; \
     rel/riak/bin/riak start; \
    sleep 20; \
     rel/riak/bin/riak-admin cluster join riak@${privateIps[0]}; \
    sleep 10; \
" 2>&1 | awk '{ print "'$i': "$0 }' ) &
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
                    


