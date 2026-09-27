#!/bin/sh
# SPEC §28.4 F2: 5 alternating rounds; interleave (default) vs NETON_IO_RING_PRIORITY=strict; epoll and io_uring.
cd /root/bench/s3
for r in 1 2 3 4 5; do
  for d in epoll iouring; do
    for m in interleave strict; do
      P=""; [ $m = strict ] && P=strict
      echo "=== round=$r driver=$d mode=$m load=$(cut -d" " -f1 /proc/loadavg)"
      NETON_IO_DRIVER=$d NETON_IO_RING_PRIORITY=$P NETON_IO_STATS=1 taskset -c 0,1 ./fairnessProbe.kexe 1024 10000 30 19400 2>&1 | grep -v "\"rounds\":[0-9]\{1,5\},"
    done
  done
done
echo F2-DONE
