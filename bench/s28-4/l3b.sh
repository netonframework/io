#!/bin/sh
# SPEC §28.4 L3, second design: the lines service suspends 1 ms per request (NETON_IO_ECHO_DELAY_MS=1, a stand-in
# for a backend call) so admission (64 permits, 250 ms wait) actually binds; maxConnections 2000.
# Per driver: C = median closed-loop throughput (1000 cold + 64 hot) over 3 runs; then 3 rounds of open-loop 2C
# (1000 cold x 20 req/s + 64 hot sharing the rest) with reconnect and a 250 ms client send deadline, RSS sampled
# every 0.2 s, admission statistics from the server, and a closed-loop recovery run with a per-second timeline.
set -u
cd /root/bench/s3; ulimit -n 65536
wait_port(){ for _ in $(seq 1 100); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done; echo "never bound $1" >&2; return 1; }
rss(){ awk '/^VmRSS/{print $2}' /proc/$1/status 2>/dev/null; }
G=./echo-client-mass-l3
start(){ NETON_IO_DRIVER=$1 NETON_IO_ECHO_MODE=lines NETON_IO_ECHO_DELAY_MS=1 NETON_IO_ADMISSION=64 NETON_IO_ADMISSION_WAIT_MS=250 \
    NETON_IO_MAX_CONNECTIONS=2000 taskset -c 0,1 ./echoServer-l3b.kexe 127.0.0.1 18090 2 > /tmp/l3-srv.log 2>&1 & pid=$!
  wait_port 18090 || { kill $pid; return 1; }
  spid=$(ss -ltnpH "sport = :18090" | grep -o "pid=[0-9]*" | head -1 | cut -d= -f2); }
stop(){ kill -TERM $pid; wait $pid 2>/dev/null; sleep 2; }
for D in epoll iouring; do
  echo "# driver $D: C"
  cs=""
  for r in 1 2 3; do
    start $D || continue
    q=$(taskset -c 2,3 $G 127.0.0.1:18090 --secs 20 --warmup 3 --threads 2 --payload 64 --class cold:1000:1:0 --class hot:64:16:0 | awk '/^qps/{print $2}')
    stop; echo "C run $r: $q"; cs="$cs $q"
  done
  C=$(echo $cs | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
  hot=$(awk -v c=$C "BEGIN{printf \"%.1f\", (c*2-20000)/64}")
  echo "# driver $D: C=$C offered=$((C*2)) hot_rate=$hot"
  for r in 1 2 3; do
    start $D || continue
    rss0=$(rss $spid); echo 0 > /tmp/l3-rssmax
    ( m=0; while kill -0 $spid 2>/dev/null; do v=$(rss $spid); [ -n "$v" ] && [ "$v" -gt "$m" ] && { m=$v; echo $m > /tmp/l3-rssmax; }; sleep 0.2; done ) & sampler=$!
    over=$(taskset -c 2,3 $G 127.0.0.1:18090 --secs 30 --warmup 3 --threads 2 --payload 64 --reconnect 1 --send-deadline-ms 250 \
      --class cold:1000:1:20 --class hot:64:16:$hot)
    rec=$(taskset -c 2,3 $G 127.0.0.1:18090 --secs 10 --warmup 0 --threads 2 --payload 64 --timeline 1 --class cold:1000:1:0 --class hot:64:16:0)
    rssmax=$(cat /tmp/l3-rssmax)
    stop; kill $sampler 2>/dev/null; wait $sampler 2>/dev/null
    echo "## $D round $r rss0_kib=$rss0 rssmax_kib=$rssmax $(echo "$over" | awk '/^gen_cpu/{print "gen_cpu=" $2}')"
    echo "$over" | grep "^class"
    grep "^admission" /tmp/l3-srv.log
    echo "recovery $(echo "$rec" | grep "^timeline_qps") $(echo "$rec" | awk '/^gen_cpu/{print "gen_cpu=" $2}')"
  done
done
echo L3B-DONE
