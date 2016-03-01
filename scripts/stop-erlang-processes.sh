#!/bin/bash

riak_dir=/home/ubuntu/Riak/riak
unset privateIps
while IFS= read -r line; do
    privateIps+=("$line")
done <  riakclients.config

for i in ${privateIps[@]}; do
echo inside$i
    (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    ./copy-erlang-stop.sh; \


" 2>&1 | awk '{ print "'$i': "$0 }' ) &
done

echo "all executed"
