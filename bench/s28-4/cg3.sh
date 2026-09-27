#!/bin/sh
# §28.11 step 3: Ir/request of fdc99f6 (base) vs this step (new, and new with strict ring priority).
# (Ir15 - Ir5) / (req15 - req5); env vars before valgrind (see the cachegrind notes).
cd /root/bench/s3; ulimit -n 65536
wait_port(){ for _ in $(seq 1 600); do nc -z 127.0.0.1 18090 2>/dev/null && return 0; sleep 0.1; done; return 1; }
run(){ tag=$1; secs=$2; vars=$3; bin=$4
  rm -f cg3-$tag-$secs.out
  sh -c "exec env NETON_IO_RUN_SECONDS=$((secs + 20)) $vars valgrind --tool=cachegrind --cache-sim=no --cachegrind-out-file=cg3-$tag-$secs.out ./$bin 127.0.0.1 18090 1" > /dev/null 2>&1 & pid=$!
  wait_port || { echo "$tag never bound"; kill $pid; return; }
  req=$(../client-fair-v2 127.0.0.1:18090 12 $secs 128 1 | awk '/^requests/{print $2}')
  wait $pid
  echo "$tag secs=$secs requests=$req Ir=$(grep -E '^summary:' cg3-$tag-$secs.out | awk '{print $2}')"
}
for d in epoll iouring; do for m in raw lines; do
  for v in base new new-strict; do
    bin=echoServer-$v.kexe; extra=""
    [ $v = new-strict ] && { bin=echoServer-new.kexe; extra="NETON_IO_RING_PRIORITY=strict"; }
    for s in 5 15; do run $v-$d-$m $s "NETON_IO_DRIVER=$d NETON_IO_ECHO_MODE=$m $extra" $bin; done
  done
done; done
echo CG3-DONE
