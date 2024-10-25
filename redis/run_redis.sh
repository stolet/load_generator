#!/bin/bash

if [ "$#" -ne 7 ]; then
    echo "Illegal number of parameters"
    echo "usage:"
    echo "[host, ip, n_runs, user, benchmark, bench_path, log_dir]"
    echo "benchmark=shim|demi"
    exit
fi

host=$1
ip=$2
n_runs=$3
user=$4
benchmark=$5
bench_path=$6
log_dir=$7

rates=()
for (( i=100000; i<=1700000; i+=200000 )); do
    rates+=($i)
done

for ((i=1; i<=n_runs; i++)); do
    for rate in "${rates[@]}"; do
        echo "Rate=$rate"
        echo "Starting Redis"
        ssh ${user}@${host} "cd ${bench_path}; sudo bash start-redis.sh ${benchmark}" &
        sleep 20
        echo "Redis started"

        log_name="${log_dir}/${benchmark}_${rate}_${i}.log"
        > ${log_name}
        echo ${log_name}
        #memtier_benchmark -h ${ip} -t 1 -c 1 -o ${log_name} --ratio=0:1 --pipeline=10000000 --rate-limiting=${rate} --test-time=10 --hide-histogram
        memtier_benchmark -h ${ip} -t 5 -c 1 -o ${log_name} --ratio=0:1 --pipeline=1000000 --rate-limiting=${rate} --hide-histogram

        echo "Killing Redis"
        ssh ${user}@${host} 'PID=$(pidof redis-server); sudo kill $PID'
        sleep 3
    done
done
