#!/bin/sh
# SPEC §28.4 L2 (closed loop): 1000 cold (depth 1) + 64 hot (depth 16); reference: hot replaced by 64 cold.
# Server taskset 0,1 (2 reactors), generator taskset 2,3; 3 s warmup + 30 s; 5 rounds, servers alternate.
set -u
cd /root/bench/s3; ulimit -n 65536
wait_port(){ for _ in $(seq 1 100); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done; echo "never bound $1" >&2; return 1; }
run(){ name=$1; load=$2; srv=$3
  sh -c "$srv" >/dev/null 2>/tmp/l2-srv.err & pid=$!
  wait_port 18090 || { kill $pid 2>/dev/null; return; }
  spid=$(ss -ltnpH "sport = :18090" | grep -o "pid=[0-9]*" | head -1 | cut -d= -f2)
  c0=$(awk '{print $14+$15}' /proc/$spid/stat); w0=$(date +%s.%N)
  if [ $load = mixed ]; then cls="--class cold:1000:1:0 --class hot:64:16:0"; else cls="--class cold:1000:1:0 --class coldx:64:1:0"; fi
  out=$(taskset -c 2,3 ./echo-client-mass 127.0.0.1:18090 --secs 30 --warmup 3 --threads 2 --payload 64 $cls)
  c1=$(awk '{print $14+$15}' /proc/$spid/stat); w1=$(date +%s.%N)
  scpu=$(awk -v a=$c0 -v b=$c1 -v x=$w0 -v y=$w1 -v t=$(getconf CLK_TCK) 'BEGIN{printf "%.2f", (b-a)/t/(y-x)}')
  echo "## $name srv_cpu=$scpu $load load=$(cut -d" " -f1 /proc/loadavg) $(echo "$out" | awk "/^gen_cpu/{print \"gen_cpu=\" \$2}")"
  echo "$out" | grep "^class"
  kill $pid 2>/dev/null; wait $pid 2>/dev/null; sleep 1; }
S="taskset -c 0,1 ./echoServer.kexe 127.0.0.1 18090 2"
for r in 1 2 3 4 5; do
  echo "# round $r"
  for load in mixed ref; do
    run ne-lines $load "NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines $S"
    run ne-lines-strict $load "NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines NETON_IO_RING_PRIORITY=strict $S"
    run nu-lines $load "NETON_IO_DRIVER=iouring NETON_IO_ECHO_MODE=lines $S"
    run ne-raw $load "NETON_IO_DRIVER=epoll $S"
    run geario $load "BENCH_ADDR=127.0.0.1:18090 taskset -c 0,1 ../server-geario"
  done
done
echo L2-DONE
