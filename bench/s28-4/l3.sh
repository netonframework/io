#!/bin/sh
# SPEC §28.4 L3 (overload, open loop 2C) with §28.12 admission (64 permits, 250 ms wait) and maxConnections 2000.
# Client: reconnect after a server close, 250 ms send deadline (see the L3 revision in the SPEC). RSS sampled
# every 0.2 s. Right after the overload, a closed-loop run (1000 cold + 64 hot) with a per-second timeline
# checks recovery. Args: "name=C" per server config. 3 rounds.
set -u
cd /root/bench/s3; ulimit -n 65536
wait_port(){ for _ in $(seq 1 100); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done; echo "never bound $1" >&2; return 1; }
rss(){ awk '/^VmRSS/{print $2}' /proc/$1/status 2>/dev/null; }
for r in 1 2 3; do
  echo "# round $r"
  for nc in "$@"; do
    name=${nc%%=*}; C=${nc#*=}
    case $name in ne-lines) D=epoll;; nu-lines) D=iouring;; esac
    NETON_IO_DRIVER=$D NETON_IO_ECHO_MODE=lines NETON_IO_ADMISSION=64 NETON_IO_ADMISSION_WAIT_MS=250 NETON_IO_MAX_CONNECTIONS=2000 \
      taskset -c 0,1 ./echoServer-l3.kexe 127.0.0.1 18090 2 > /tmp/l3-srv.log 2>&1 & pid=$!
    wait_port 18090 || { kill $pid; continue; }
    spid=$(ss -ltnpH "sport = :18090" | grep -o "pid=[0-9]*" | head -1 | cut -d= -f2)
    rss0=$(rss $spid)
    ( m=0; while kill -0 $spid 2>/dev/null; do v=$(rss $spid); [ -n "$v" ] && [ "$v" -gt "$m" ] && { m=$v; echo $m > /tmp/l3-rssmax; }; sleep 0.2; done ) & sampler=$!
    hot=$(awk -v c=$C "BEGIN{printf \"%.1f\", (c*2-20000)/64}")
    over=$(taskset -c 2,3 ./echo-client-mass-l3 127.0.0.1:18090 --secs 30 --warmup 3 --threads 2 --payload 64 \
      --reconnect 1 --send-deadline-ms 250 --class cold:1000:1:20 --class hot:64:16:$hot)
    rec=$(taskset -c 2,3 ./echo-client-mass-l3 127.0.0.1:18090 --secs 10 --warmup 0 --threads 2 --payload 64 --timeline 1 \
      --class cold:1000:1:0 --class hot:64:16:0)
    rssmax=$(cat /tmp/l3-rssmax)
    kill -TERM $pid; wait $pid 2>/dev/null; kill $sampler 2>/dev/null; wait $sampler 2>/dev/null
    echo "## $name C=$C offered=$((C*2)) rss0_kib=$rss0 rssmax_kib=$rssmax $(echo "$over" | awk '/^gen_cpu/{print "gen_cpu=" $2}')"
    echo "$over" | grep "^class"
    grep "^admission" /tmp/l3-srv.log
    echo "recovery $(echo "$rec" | grep "^timeline_qps") $(echo "$rec" | awk '/^gen_cpu/{print "gen_cpu=" $2}')"
    sleep 2
  done
done
echo L3-DONE
