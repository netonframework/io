#!/bin/sh
# L1 diagnosis for the line-framed mode (SPEC §28.4): is the cold p99 inflation the GC?
# Variants: cur (default), NoGc (gc=noop), MarkSt (single-threaded mark); cur + GC stats separately.
set -u
cd /root/bench/s3; ulimit -n 65536
wait_port(){ for _ in $(seq 1 100); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done; echo "never bound $1" >&2; return 1; }
run(){ name=$1; srv=$2
  sh -c "$srv" >/tmp/d1-srv.log 2>&1 & pid=$!
  wait_port 18090 || { kill $pid 2>/dev/null; return; }
  out=$(taskset -c 2,3 ./echo-client-mass 127.0.0.1:18090 --secs 30 --warmup 3 --threads 2 --payload 64 --class cold:1000:1:20 --class hot:64:16:3523.4)
  echo "## $name $(echo "$out" | awk "/^gen_cpu/{print \"gen_cpu=\" \$2}")"
  echo "$out" | grep "^class cold"
  kill $pid 2>/dev/null; wait $pid 2>/dev/null
  grep "gc-stats" /tmp/d1-srv.log | tail -1
  sleep 1; }
S="127.0.0.1 18090 2"
for r in 1 2 3; do
  echo "# round $r"
  run cur "NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines taskset -c 0,1 ./echoServer-cur.kexe $S"
  run nogc "NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines taskset -c 0,1 ./echoServer-NoGc.kexe $S"
  run markst "NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines taskset -c 0,1 ./echoServer-MarkSt.kexe $S"
  run cur-gcstats "NETON_IO_GC_STATS=1 NETON_IO_DRIVER=epoll NETON_IO_ECHO_MODE=lines taskset -c 0,1 ./echoServer-cur.kexe $S"
done
echo D1-DONE
