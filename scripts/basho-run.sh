#!/bin/bash
unset nodes
while IFS= read -r line; do
    nodes+=("$line")
done <  riaknodes.config

Type="basho_bench"
Counter=0
bench_path=/root/Bench/basho_bench
for i in ${nodes[@]}; do
echo $i;
 (ssh -i chathu-keypair.pem  -o StrictHostKeyChecking=no root@$i "\
     cd $bench_path; \
     ./basho_bench examples/riakclient.config; \ 
" 2>&1 | awk '{ print "'$i': "$0 }' ) &
done
echo $Counter
echo "finished client configs"

