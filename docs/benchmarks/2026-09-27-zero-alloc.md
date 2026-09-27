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
