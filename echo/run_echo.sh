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
for (( i=130000; i<=130000; i+=5000 )); do
    rates+=($i)
done

for ((i=1; i<=n_runs; i++)); do
    for rate in "${rates[@]}"; do
        echo "Run=$i Rate=$rate"
	echo "Starting server"
        ssh ${user}@${host} "cd ${bench_path}; sudo bash start-server.sh ${benchmark} ${ip}" &
        sleep 4
        echo "Server started"

	sudo LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu ./build/load-generator \
	    -a d8:00.0 -n 1 -c 0xff -- \
            -d uniform -r ${rate} -f 1 -s 128 -t 10 -e 37 \
            -c addr.cfg -o output.dat -D constant -i 0 -j 0 -m 0

	echo "Killing server"
	if [ "$benchmark" == "shim" ]; then
	    ssh ${user}@${host} 'PID=$(pidof shim-tcp-ping-pong.elf); sudo kill $PID'
	elif [ "$benchmark" == "demi" ]; then
	    ssh ${user}@${host} 'PID=$(pidof tcp-ping-pong.elf); sudo kill $PID'
	fi
	echo "Killing any ports that didn't close"
	ssh ${user}@${host} 'sudo kill $(sudo lsof -t -i :56789)'

	sudo chown $user output.dat
	python3 parse_lat.py | tee $log_dir/${benchmark}_${rate}_${i}.log
	rm output.dat
	sleep 2
    done
done
