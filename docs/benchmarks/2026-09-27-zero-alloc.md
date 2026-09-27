# SPEC §24: zero-allocation read/write path (153, 2026-09-27)

Raw: `2026-09-27-153-{p0,p1,p2,v35,v36,v37,v38,p4}-raw.txt`. Client `echo-client-fair`, 128 B, fair
service (Jain ≥ 0.94 everywhere). geario is its default build (io_uring). Ratios are paired medians
against geario in the same run.

## Why: the GC

Per request on one pinned core, v34 made 0.84 (epoll) / 1.13 (io_uring) `sched_yield` calls, all from
Kotlin/Native's `MainGCThread::PerformFullGC` waiting for the reactor to reach a safepoint (gdb). With
GC disabled the same code gained 8 % (epoll) and 13 % (io_uring) and reached geario. The GC ran that
often because every request allocated: ~3 coroutine continuations, a boxed Int result, a new pin.

## What changed, by build

| build | change | yields/req e / u | allocs/req | Ir/req e / u |
|---|---|---|---|---|
| v34 | before §24 | 0.84 / 1.13 | ~3–4 | 3391 / 4561 |
| v35 | tail-call read/write, reactor completes parked ops, boxed-Int cache | 0.16 / 0.36 | ~1–2 | 2831 / 3373 |
| v36 | drained buffers keep their array (idle sweep returns it), pin per direction, uring read split | 0.08 / 0.07 | 1.0 | 2407 / 3020 |
| v37 | allocation-free poller interface, hang-up flag | 0.07 / 0.07 | 1.0 | 2421 / 3086 |
| v38 | fast-path results return the shared box | **0.000 / 0.000** | **0** | **2325 / 2842** |

geario: ~4050 Ir/req. io_uring syscalls per request: v34 1.23, v38 0.094 (geario 0.29).

## Throughput against geario (v38)

| operating point | epoll | epoll + short-read rule | io_uring |
|---|---|---|---|
| pinned core, 12 conns (8 rounds) | 0.984 (3/8) | 1.051 (7/8) | **1.022 (8/8)** |
| ×4, 100 conns (4 rounds) | 0.962 (1/4) | 0.888 (0/4) | 0.975 (1/4) |
| ×4, 1000 conns (3 rounds) | 0.992 (1/3) | 0.959 (1/3) | 1.005 (2/3) |

The ×4 rows are noisy on this host (geario's own median moved 256k → 329k between runs). A separate
×4 / 100-connection profile (3 rounds, `p4`) had io_uring ahead of geario in every round (306k vs 283k,
297k vs 267k, 269k vs 233k) at lower CPU per request (6.6–7.5 µs vs 7.2–8.8 µs, 0.042 vs 0.126
syscalls per request).

**Standing:** io_uring (the Linux default) is at or above geario at every operating point measured.
epoll (the fallback, e.g. under Docker's default seccomp) is at 0.96–0.99; its cost is ~3 syscalls per
request (recv, the EAGAIN recv that confirms the socket is drained, send). The short-read rule removes the
EAGAIN recv and wins on one core but loses on four (a fast client's next request is often already there);
next: make it adaptive per connection.

## Later changes (v39–v42)

- epoll short-read rule (SPEC §24.5): decided per reactor, speculation skipped above a 90 % miss rate.
  v41: medians match the better fixed mode at every point (pinned 126.6k vs always 125.7k / off 112.0k;
  ×4 100 conns ≈ off; ×4 1000 conns 234k vs off 221k), Jain ≥ 0.976.
- `Framed.serveLoop` / `send` / `receiveEach` allocate nothing of their own (inline bodies); used by msgtrans,
  whose rpc path went from 8 to 3 allocations per request (msgtrans SPEC §12).
- io_uring: a cancelled or timed-out send is cancelled in the kernel and accounted before its writer resumes
  (found by msgtrans' contract tests; `WriteCancelTest`).
- v42 acceptance (`2026-09-27-153-v42-raw.txt`): tests pass on all three Linux drivers (73) and msgtrans in
  both write modes (41). That run's throughput was lower than v38–v41 (io_uring pinned 0.986, ×4 0.967); the
  deterministic check says it is the host, not the code — cachegrind Ir/request v38 vs v42: epoll 2328 / 2326,
  io_uring 2956 / 2944, and GC yields per request stay 0.

## io_uring reads straight into the user buffer (SPEC §24.7, v43–v45)

The payload matrix (`…matrix2-raw.txt`) showed io_uring behind geario only at 64 KB (0.80–0.89): multishot with
16 KB provided buffers splits each message into ≥ 4 CQEs with a copy each. Reads now go straight into the caller's
buffer (one RECV, allocation-free, completed by the reactor), with `IORING_RECVSEND_POLL_FIRST` — without it about
10 % of connections were starved at 64 KB / 1000 connections (`…fair64-raw.txt`, `…v44-pf-raw.txt`).

v45 (`…v45-raw.txt`; paired vs geario, 4 rounds; tests: neton-io 73 × 3 drivers, msgtrans 41):

| payload | pinned, 12 conns | ×4, 100 conns | ×4, 1000 conns |
|---|---|---|---|
| 128 B | io_uring 0.986 (1/4), epoll 0.933 | 1.000 (2/4), 0.945 | **1.096 (4/4)**, 1.056 (4/4) |
| 4 KB | **1.240 (4/4)**, 1.277 (4/4) | **1.125 (4/4)**, 1.081 (4/4) | **1.194 (4/4)**, 1.161 (4/4) |
| 64 KB | **1.777 (4/4)**, 1.679 (4/4) | **1.372 (4/4)**, 1.334 (4/4) | **1.366 (4/4)**, 1.342 (4/4) |

Fairness at 64 KB / 1000 connections, server on cores 0–1 and client on 2–3: io_uring min completions per
connection 244–399 (Jain 0.94–0.96), epoll 282–333 (0.87–0.92), geario 255–276 (0.999). Memory at ×4 also dropped
(no provided-buffer pool: ≈ 20 MB instead of ≈ 46 MB at 100 connections).

Open: 128 B at one pinned core and at ×4/100 is a tie (0.986–1.000); in the v43 run without POLL_FIRST it was
1.18 / 0.99. geario keeps better fairness at 64 KB / 1000 (0.999 vs 0.94–0.96).

## macOS (kqueue), this Mac (10 cores, shared, load 5–10)

neton `echoServer` with 10 reactors vs geario with its default 10 workers; `echo-client-fair` on the same host;
4 rounds alternated (`2026-09-27-mac-matrix-raw.txt`).

| payload | 12 conns | 100 conns | 1000 conns |
|---|---|---|---|
| 128 B | 1.017 (4/4) | 0.999 | 1.004 |
| 4 KB | 1.016 (4/4) | 1.001 | 1.002 |
| 64 KB | **1.748 (4/4)** | **1.579 (4/4)** | **1.758 (4/4)**; p99 27 ms vs 356 ms |

At 128 B and 4 KB both servers sit at the same ≈ 136–143k req/s whatever the connection count: the client caps
the run, so throughput cannot rank them there. Server CPU per request does (100 conns, 3 rounds each): **kqueue
13.5–13.8 µs vs geario 15.0–15.3 µs at 128 B, 13.8–14.1 vs 15.8–16.2 µs at 4 KB — 10–13 % less.**

Found on the way: macOS `nc -z` resets its connection; the read error escaped the connection coroutine and took
down the reactor (fixed; `ConnectionFaultTest`).
