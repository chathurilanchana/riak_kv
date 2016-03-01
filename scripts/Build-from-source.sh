#!/bin/bash

riak_dir=/root/Riak/riak

#put cleanips and private ips in relavant order
hb_freq=1000
old_hb="ordering_service_hb_freq=3000"

unset privateIps
while IFS= read -r line; do
    privateIps+=("$line")
done <  riaknodes.config

wait
for i in ${privateIps[@]}; do
echo inside$i
    (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    cd $riak_dir; \
    rel/riak/bin/riak stop; \
    sleep 5; \
    rm -r rel/riak; \
    cd deps; \
    rm -r riak_kv; \
    git clone -b Riak-Kv-Causal-dev https://github.com/chathurilanchana/riak_kv.git; \
    wait; \
    cd ..; \
    sleep 3; \
    make rel; \
    sleep 60; \
    ulimit -n 65536; \
    echo $riak_dir; \
    sed -i s/'127.0.0.1'/$i/g $riak_dir/rel/riak/etc/riak.conf; \
     sudo sed -i -e \"s/ordering_service_hb_freq.*/ordering_service_hb_freq= $hb_freq/\" $riak_dir/rel/riak/etc/riak.conf; \
    rel/riak/bin/riak start; \
    sleep  10; \
    rel/riak/bin/riak-admin test; \
    rel/riak/bin/riak-admin cluster join riak@${privateIps[0]}; \

" 2>&1 | awk '{ print "'$i': "$0 }' ) &
echo "before sleep"
sleep 120;
echo "woke-up"
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
