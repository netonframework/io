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
