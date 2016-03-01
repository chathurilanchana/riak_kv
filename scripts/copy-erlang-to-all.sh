#!/bin/bash

riak_dir=/home/ubuntu/Riak/riak
unset privateIps
while IFS= read -r line; do
    privateIps+=("$line")
done <  riakclients.config

wait
for i in ${privateIps[@]}; do

echo inside$i
 scp -i chathu-keypair.pem  -o StrictHostKeyChecking=no copy-erlang-stop.sh root@$i:/root;

done
echo "all executed"
