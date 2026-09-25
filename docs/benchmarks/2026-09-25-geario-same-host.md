# neton-io vs geario, same host, same client (2026-09-25)

Goal: a fair, reproducible baseline for the "match geario" program. Everything the earlier README
table could not control is controlled here: one host, one client binary, identical parameters,
alternating rounds.

## Setup

| | |
|---|---|
| Host | 106.55.63.153, Rocky Linux 9.8, kernel 5.14.0-687.44.1, 4 vCPU (AMD EPYC KVM), load 0.00 at start |
| io_uring | `kernel.io_uring_disabled=0` set for the session (the kernel update had turned it to 2) |
| Client | geario `bench-echo/client` (blocking std sockets, one thread per connection); args `addr conns secs payload` |
| Params | payload 128 B, 8 s per round, 8 rounds, warm-up round discarded; operating points 1 conn (latency) and 12 conn (throughput knee, per geario's sweep) |
| neton-io | `echoServer.kexe`, release, linuxX64, cross-compiled from macOS; `NETON_IO_DRIVER=epoll` / `iouring`; single reactor |
| geario | `server-geario` (polling) and `server-geario-uring-real` (`--features geario/neon-uring`), release, cross-compiled; server workers = `available_parallelism` = 4 |
| Raw | `2026-09-25-153-round1-raw.txt`, `2026-09-25-153-round2-raw.txt`, `2026-09-25-153-round3-raw.txt` |

Round 1 started with io_uring disabled, so its neton-uring rows are only present for the 12-conn
half and its "geario-uring" is the polling binary (byte-identical). Round 2 is the clean run.

## Round 2 (io_uring enabled, real uring geario) — medians of 8

| server | 1 conn qps | 1 conn p50 | 12 conn qps | 12 conn p50 |
|---|---|---|---|---|
| neton epoll | 32,279 | 27.5 µs | 104,426 | 113.8 µs |
| neton io_uring | 31,253 | 28.7 µs | 129,076 | 91.3 µs |
| geario polling | 31,178 | 28.5 µs | 286,950 | 35.5 µs |
| geario uring | 31,766 | 28.1 µs | 303,652 | 36.1 µs |

Spread (min..max) at 12 conns is roughly ±15% for every server; the 2.4–2.8× gap is far outside it.

## Reading

1. **At one connection the four are indistinguishable** (31–32k, p50 27–29 µs). With no queue, a
   request's cost is the recv/send/wait syscall floor, and neton-io already sits on it. Nothing
   in neton-io's per-request user-space path is the problem.
2. **The whole gap is under concurrency.** geario runs 4 workers (one per core); neton-io runs one
   reactor on one core. Round 3 pins every server to a single core to separate core count from
   per-core efficiency — see below.
3. **neton io_uring beats neton epoll by ~24% at 12 conns** on this host (129k vs 104k). The
   README's "io_uring ~10% slower than epoll" came from a 2-core box and is retired by this data.

## Round 3 (all servers pinned to core 1 with `taskset`, client on the other cores) — medians of 8

| server | 1 conn qps | 1 conn p50 | 12 conn qps | 12 conn p50 |
|---|---|---|---|---|
| neton epoll | 32,499 | 27.4 µs | 107,062 | 99.0 µs |
| neton io_uring | 29,392 | 29.2 µs | 104,350 | 93.4 µs |
| geario polling | 32,119 | 28.3 µs | 140,655 | 77.2 µs |
| geario uring | 32,482 | 28.2 µs | 146,506 | 74.0 µs |

## Decomposition of the 2.8× gap (12 conns)

| factor | geario | neton | ratio |
|---|---|---|---|
| one core each | 141–147k | 104–107k | **1.35–1.4×** — per-core efficiency |
| all 4 cores | 287–304k | 104–129k | **2.4–2.8×** — neton does not scale past one reactor |

So roughly 2× of the gap is core count (geario's 4 workers vs neton's single reactor) and 1.35×
is per-core cost. Two further observations:

- neton io_uring's +24% at 12 conns (round 2) vanishes when pinned to one core (round 3:
  104k vs epoll 107k). Unpinned, io_uring's kernel workers ran on the other cores, so that gain
  was extra cores, not a cheaper per-request path. On one core the current naive uring
  (one SQE per enter, pin per op) is no better than epoll.
- geario gains only ~4% from uring on one core; its per-core edge over neton is in the readiness
  path itself (no per-round re-arm, no speculative EAGAIN recv), not in io_uring.

## What this decides (evidence-ranked)

1. **Multi-reactor** (one reactor per core, accept distributed round-robin, connection pinned to
   its reactor for life): worth ~2× on this host. Fits SPEC §3.3 and msgtrans's reactor-bound
   single-owner contract unchanged.
2. **Per-core readiness path** (~1.35×): arm-once / edge-triggered instead of per-round re-arm;
   read-before-arm policy (one wasted EAGAIN recv per request was counted earlier with
   NETON_IO_STATS). Single-variable each, measured with this harness pinned to one core.
3. **io_uring done properly** (batched submission, registered buffers, multishot): only after 1–2,
   and judged on one core — otherwise its "gain" is just borrowed cores.

## Round 4 (multi-reactor acceptance, unpinned, 4 cores) — medians of 8

neton `echoServer` with 1 vs 4 reactors (SPEC §16, commit aa2b42c; readiness path still pre-§17) against geario's 4 workers.

| server | 12 conn qps (min..max) | 12 conn p50 | 48 conn qps (min..max) | 48 conn p50 |
|---|---|---|---|---|
| neton-epoll-1 | 112,398 (88,927..118,403) | 101.3 µs | 104,294 (86,358..118,441) | 438.9 µs |
| neton-epoll-4 | 317,992 (284,398..346,632) | 10.6 µs | 335,620 (326,539..355,509) | 10.6 µs |
| neton-uring-1 | 118,478 (107,200..130,646) | 100.1 µs | 116,054 (91,214..134,073) | 413.4 µs |
| neton-uring-4 | 205,226 (183,670..227,179) | 46.5 µs | 235,780 (210,807..256,933) | 163.2 µs |
| geario | 275,566 (222,708..328,560) | 35.6 µs | 298,337 (243,052..320,652) | 149.3 µs |
| geario-uring-real | 275,696 (220,390..310,025) | 38.2 µs | 309,508 (248,478..340,026) | 140.0 µs |

**Reading.** Four epoll reactors take neton from ~110k to ~318k at 12 connections (2.9×) and to
~335k at 48, matching or slightly exceeding geario's 4 workers (275–317k) on the same host with the
same client. The core-count half of the gap is closed. io_uring ×4 (~215k) is *slower* than
epoll ×4: the naive uring driver's kernel io-wq threads now compete with the four reactors for the
same four cores, so uring stays lever #3 until it submits in batches with registered buffers.
The very low p50 for neton-epoll-4 (10–11 µs, below the 27 µs single-connection floor) is measured
by the same client and is reported as-is, but it is physically odd — likely a client-thread
scheduling artifact at this concurrency — and is not used as evidence for anything.

## Round 5 (SPEC §17 single-variable: level-triggered re-arm vs persistent edge-triggered) — medians of 8

`neton-lt` = `echoServer.kexe` at aa2b42c (one-shot interest re-armed per round);
`neton-et` = the same at d51d51e (§17: interest registered once, EPOLLET, per-fd ready flag,
drain to EAGAIN). epoll only. Raw: `2026-09-25-153-round5-raw.txt`.

| operating point | neton-lt | neton-et | geario | et/lt |
|---|---|---|---|---|
| pinned core 1, 1 reactor, 1 conn | 31,613 / 27.3 µs | 31,360 / 27.5 µs | 31,743 / 28.6 µs | — |
| pinned core 1, 1 reactor, 12 conn | 101,686 / 106.8 µs | 104,196 / 104.0 µs | 131,790 / 83.2 µs | 1.02× |
| unpinned, 4 reactors, 12 conn | 319,851 / 10.8 µs | 332,224 / 10.5 µs | 269,313 / 38.7 µs | 1.04× |
| unpinned, 4 reactors, 48 conn | 331,810 / 10.4 µs | 336,551 / 10.7 µs | 280,080 / 148.6 µs | 1.01× |

`NETON_IO_STATS` for neton-et, pinned, 12 conns, 8 s (part C): 762,578 requests (= writes − 1 per
connection); **reads 1,525,923, reads_would_block 762,578 — exactly one EAGAIN recv per request**;
polls 75,685 (0.10 per request, ~10 events per `epoll_wait`); writes_would_block 0.

**Reading.** §17 removed the per-request `epoll_ctl` and made `epoll_wait` cheap (0.1/req), but that
was worth only 2–4%, inside the run-to-run spread. The per-request syscall budget is now
recv(OK) + send + recv(EAGAIN) + 0.1 epoll_wait ≈ 3.1, and the wasted recv is the whole remaining
per-core difference to geario (1.27× pinned). SPEC §17's acceptance line expected
`reads_would_block/req` to fall to ~0; that expectation was wrong for its own design ("recv until
EAGAIN") — draining to EAGAIN *guarantees* one EAGAIN per burst. Removing it needs the short-read
rule (a recv that returns fewer bytes than requested counts as drained; safe under EPOLLET/EV_CLEAR
because every new arrival raises a fresh edge), which is the next single-variable step (§17b).

## Round 6 / 6b (SPEC §17b short-read rule) — rejected

`neton-sr` = §17 plus "recv shorter than offered ⇒ drained, clear the flag, no EAGAIN recv" (7cceced).
Raw: `2026-09-25-153-round6-raw.txt`, `2026-09-25-153-round6b-raw.txt`.

| operating point | neton-et | neton-sr | geario |
|---|---|---|---|
| pinned core 1, 1 reactor, 12 conn | 102,870 | 105,396 | 126,885 |
| unpinned, 1 reactor, 12 conn (6b, 4 rounds) | 100–128k | 101–133k | — |
| unpinned, 4 reactors, 12 conn | 327,560 | **217,513** | 276,078 |
| unpinned, 4 reactors, 48 conn | 340,492 | **237,522** | 297,696 |

STATS pinned (part C): sr has `reads` 870,503 ≈ `writes` 870,490, `reads_would_block` 0 — the wasted
recv is gone — and throughput is unchanged. STATS unpinned ×4 (6b), per reactor:

| | et-4 (reactor 0) | sr-4 (reactor 0) |
|---|---|---|
| writes (= requests) | 667,251 | 518,866 |
| events / task runs | 60,891 / 60,896 | 518,873 / 518,878 |
| polls | 39,945 | 195,575 |
| reads_would_block | 60,884 | 0 |

**Reading.** Under `et`, one wake serves ~11 requests: after a successful recv the ready flag stays
set, and the next recv finds the *next* request already queued. That happens because the host is
oversubscribed (4 reactors + 12 client threads on 4 cores): `send()` sync-wakes the client thread,
which preempts the reactor, does its recv/send and blocks; the reactor resumes and its recv hits.
`sr` parks after every short read and pays one `epoll_wait` per request (polls ≈ requests), hence
−33%. Pinned to one core there is no such ping-pong (both variants see one EAGAIN per request) and
the removed recv is worth nothing measurable. Decision: keep drain-to-EAGAIN; §17b reverted.
This also explains the ~10 µs p50 at 4 reactors (request/response completes by on-core hand-off),
and it means the 4-reactor 330k figure partly reflects this scheduler effect — geario runs under the
identical setup and gets 276k, so the comparison stays fair, but the number is a property of
"server and client share 4 cores", not of the reactor alone.

The per-core pinned gap (neton ~104k vs geario ~127–132k, 1.25×) is therefore *not* readiness-path
syscall count. Next: count syscalls per request on both servers (`strace -c`) and profile the
pinned reactor (`perf`) before touching anything else.

## Profiling the pinned single-core gap (perf on 153) — raw in `2026-09-25-153-profile-pinned-raw.txt`

Tools that work on this KVM guest: `perf trace -s` (syscall counts) and `perf record -e cpu-clock`
(software sampling). Hardware counters are garbage here (`instructions` 3.8e16), so `perf stat`
cycles/instructions and cycle-based `perf record` are unusable; `strace -c` slows the server so
much that it changes the regime (clients always ahead, 8 `epoll_wait` per 43k requests).

**1. geario is an io_uring server on this host — in every round.** `server-geario` syscalls in a
3 s window: `io_uring_enter` 36,537, eventfd `read`/`write` 36.5k each, nothing else — ~0.27
syscalls per request. Its source confirms it: `default_reactor()` prefers
`uring::Reactor::new(2048)` (`COOP_TASKRUN | SINGLE_ISSUER | DEFER_TASKRUN`, batched
`submit_and_wait`) and falls back to polling only if setup fails. So "geario polling" in rounds
1–6 was uring too (which is why it always matched `geario-uring-real`); the labels stay as
recorded, the interpretation changes: **the per-core target is an io_uring server**.

**2. neton syscalls per request** (same window): epoll: recvfrom 1.87 (0.87 EAGAIN), sendto 1,
epoll_wait 0.075 (~13 events per wait) ≈ 2.95/request. io_uring: `io_uring_enter` 38,570 per 3 s —
about the same count as geario — so **neton-uring is not losing on syscalls**.

**3. Where the pinned core goes** (`perf record -e cpu-clock`, 3 s, all pinned to core 1; the
sampling itself costs every server ~10–20%):

| server | kernel, reactor thread | user, reactor thread | **GC thread** | qps under perf |
|---|---|---|---|---|
| geario (uring) | 93.1% | 6.8% (+0.2% libc) | — | 106k |
| neton epoll | 78.9% | 11.7% (+1.9% libc) | **7.4%** | 95k |
| neton io_uring | 68.3% | 15.4% (+0.2% libc) | **16.1%** | 105k |

Top kernel symbol for all three is `_raw_spin_unlock_irqrestore` under `sock_def_readable`
(24–28%: waking the client thread on the other core — the cost of the benchmark's own client) and
`nft_do_chain` (~5%: firewalld's nftables on loopback). Both are paid equally by every server.

**Reading.** Per request, neton-uring already spends *less* kernel time than geario. The whole
remaining deficit is on the Kotlin/Native side: a "Main GC thread" that burns 7–16% of the pinned
core (`sched_yield` 15–54k per 3 s, `__schedule`/`do_sched_yield` in the profile) plus 12–15% user
time against geario's 7%. So the next single-variable experiments are runtime-side, not
syscall-side: GC configuration (fixed larger target heap; stop-the-world GC with no separate
thread), then an allocation-free hot path. Only after that is the uring driver's own setup
(`COOP_TASKRUN|SINGLE_ISSUER|DEFER_TASKRUN`, multishot recv, provided buffers) worth a round —
the kernel supports them (features 0x1ffff, last_op 57, `IORING_RECV_MULTISHOT` in the UAPI).

## Round 7 (SPEC §17c step 1: GC configuration) — pinned core 1, 1 reactor, 12 conns, medians of 8

`et` = default GC (concurrent, autotuned); `gc64` = `NETON_IO_GC_TARGET_MB=64` (autotune off);
`stw` = same code built with `-Xbinary=gc=stwms`. Raw: `2026-09-25-153-round7-raw.txt`. The host
was noisier than in rounds 5–6 (geario 120k here vs 127–132k), so compare within the round only.

| driver | et | gc64 | stw | geario |
|---|---|---|---|---|
| epoll | 97,594 (85.8k..118.4k) | **106,732** (94.7k..120.6k) | 102,185 (91.5k..115.7k) | 120,062 (96.5k..127.0k) |
| io_uring | 93,518 (84.9k..105.3k) | 97,532 (85.4k..103.0k) | 90,997 (79.0k..97.0k) | — |

**Reading.** A fixed larger target heap is worth up to +9% (fewer collections); stop-the-world GC
gains nothing (the work moves into the mutator). GC *settings* do not close the gap; the GC has
to be given less to do. Next: bound the prize with `-Xbinary=gc=noop` (no collection at all —
fine for an 8 s run) before spending effort on an allocation-free hot path.

## Round 7b (GC upper bound: `-Xbinary=gc=noop`) — pinned core 1, 1 reactor, 12 conns, medians of 8

Raw: `2026-09-25-153-round7b-raw.txt` (profiles by thread at the end of the file).

| driver | gc64 | **nogc** | geario |
|---|---|---|---|
| epoll | 109,997 (85.2k..122.3k) | **125,589** (92.3k..131.4k) | 127,289 (99.1k..149.6k) |
| io_uring | 105,520 (91.7k..115.6k) | 114,917 (99.8k..128.7k) | — |

Profiles (cpu-clock): epoll-gc64 — GC thread 3.6%, user 12.2%; epoll-nogc — no GC thread, user
11.4%, kernel 86.6%; uring-nogc — user 15.8%, kernel 84.0%. Top user symbols with GC off:
`HashMap.findKey`/`addKey` (boxed `Int` fd keys in the waiter map / ready set), the allocator
(`CustomAllocator::Allocate*`), `Pinned.<init>` (one pin per recv/send), the coroutine park/resume
path (`DispatchedTask.run`, `BaseContinuationImpl.resumeWith`, `JobSupport.removeNode` from
`invokeOnCancellation`), and on uring `prepSqe`/`submit`/`reap`.

**Reading.** With GC cost removed, neton epoll is at geario's level on one core (125.6k vs 127.3k,
well inside the spread). GC is therefore the entire remaining per-core gap, and it is fed by
5–8 small allocations per request. SPEC §17c step 2 (allocation-free hot path: fd-indexed arrays
instead of boxed-key hash maps, no `ReadOutcome` object, cached pins, no per-park cancellation
node) is the change to make; `gc=noop` is a bench bound, not a configuration.
