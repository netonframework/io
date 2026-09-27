#!/bin/sh
# SPEC §28.4 F3 rerun: the burst connection writes 10000 frames every 100 ms (open loop) instead of closed loop.
# L1: cold 1000 x 20 req/s + hot 64 (depth 16) sharing 0.5C - 20k; reference: cold alone.
# F3: L1 hot background + 100 cold x 20 req/s + 1 burst connection (10000 frames per write, one write every 100 ms);
#     reference: the same without the burst connection. 3 s warmup + 30 s; 5 rounds, servers alternate.
set -u
cd /root/bench/s3; ulimit -n 65536
wait_port(){ for _ in $(seq 1 100); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done; echo "never bound $1" >&2; return 1; }
srvcmd(){ S="taskset -c 0,1 ./echoServer.kexe 127.0.0.1 18090 2"
  case $1 in
    ne-lines) echo "NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines $S";;
    nu-lines) echo "NETON_IO_DRIVER=iouring NETON_IO_ECHO_MODE=lines $S";;
    ne-raw) echo "NETON_IO_DRIVER=epoll $S";;
    geario) echo "BENCH_ADDR=127.0.0.1:18090 taskset -c 0,1 ../server-geario";;
  esac; }
run(){ name=$1; test=$2; cls=$3
  sh -c "$(srvcmd $name)" >/dev/null 2>&1 & pid=$!
  wait_port 18090 || { kill $pid 2>/dev/null; return; }
  spid=$(ss -ltnpH "sport = :18090" | grep -o "pid=[0-9]*" | head -1 | cut -d= -f2)
  c0=$(awk '{print $14+$15}' /proc/$spid/stat); w0=$(date +%s.%N)
  out=$(taskset -c 2,3 ./echo-client-mass 127.0.0.1:18090 --secs 30 --warmup 3 --threads 2 --payload 64 $cls)
  c1=$(awk '{print $14+$15}' /proc/$spid/stat); w1=$(date +%s.%N)
  scpu=$(awk -v a=$c0 -v b=$c1 -v x=$w0 -v y=$w1 -v t=$(getconf CLK_TCK) 'BEGIN{printf "%.2f", (b-a)/t/(y-x)}')
  echo "## $name srv_cpu=$scpu $test load=$(cut -d" " -f1 /proc/loadavg) $(echo "$out" | awk "/^gen_cpu/{print \"gen_cpu=\" \$2}")"
  echo "$out" | grep "^class"
  kill $pid 2>/dev/null; wait $pid 2>/dev/null; sleep 1; }
for r in 1 2 3 4 5; do
  echo "# round $r"
  for nc in "$@"; do
    name=${nc%%=*}; C=${nc#*=}
    hot=$(awk -v c=$C "BEGIN{printf \"%.1f\", (c*0.5-20000)/64}")
    run $name F3 "--class hot:64:16:$hot --class cold:100:1:20 --class burst:1:10000:100000"
    run $name F3-ref "--class hot:64:16:$hot --class cold:100:1:20"
  done
done
echo F3B-DONE
