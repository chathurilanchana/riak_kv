#!/bin/bash
unset nodes
while IFS= read -r line; do
    nodes+=("$line")
done <  riakclients.config

Type="basho_bench"
Counter=0
bench_path=/root/Bench/basho_bench
duration=2
operations="[{put,1},{update,1},{get,10}]"
code_paths="[\"/root/Riak/riak/rel/riak/lib/riak_kv-2.1.1-39-g97e36c9\",\"/root/Riak/riak/rel/riak/lib/riak_core-2.1.5\"]"
riakclient_nodes="[\'riak@172.31.0.180\']"

for i in ${nodes[@]}; do
    let Counter++
    NodeName="$Type$Counter@$i"
    riakclient_mynode="[\'$NodeName\', longnames]"
echo $i;
 (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
    cd $bench_path; \
     sudo sed -i -e \"s/{duration.*/{duration, $duration}./\" $bench_path/examples/riakclient.config; \
     sudo sed -i -e \"s/{operations.*/{operations, $operations}./\" $bench_path/examples/riakclient.config; \
    sudo sed -i -e \"s/{riakclient_nodes.*/{riakclient_nodes, $riakclient_nodes}./\" $bench_path/examples/riakclient.config; \
    sudo sed -i -e \"s/{riakclient_mynode.*/{riakclient_mynode, $riakclient_mynode}./\" $bench_path/examples/riakclient.config; \
     sudo sed -i -e 's~{code_paths.*~{code_paths, $code_paths}.~' $bench_path/examples/riakclient.config; \
    # ./basho_bench examples/riakclient.config; \ 
" 2>&1 | awk '{ print "'$i': "$0 }' ) &
done
echo $Counter
echo "finished client configs"

