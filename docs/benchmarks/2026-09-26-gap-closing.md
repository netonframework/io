# SPEC §23 gap closing: measurements (153, 2026-09-26)

Host: 153 (4 vCPU VM, Rocky 9, kernel with io_uring). Client: `bench/echo-client-fair` (blocking,
one thread per connection, per-connection counts and Jain index). Server: `echoServer` release
build, raw echo unless noted. Paired rounds, order rotated.

Binaries: v25 = before §23.2; v26 = §23.2; v27 = §23.4; v28 = §23.6; v29 = §23.7 (first cut);
v30–v34 = hot-path fixes found with cachegrind (below).

## Throughput pairs are too noisy here for a 2 % question

Per-round spread on this VM is ±10–20 % (raw files `…q232…`, `…q234…`, `…q237…`), so throughput
pairs cannot confirm or rule out a 2 % hot-path cost. CPU time per request (`…q238-cpu…`) is no
better: the pinned server runs its core at 100 %, so CPU per request is 1 / throughput. The VM
exposes no hardware instruction counter.

**Method used instead**: cachegrind (valgrind 3.26, installed on 153 for this) counts user-space
instructions, deterministically. Two runs per binary (5 s and 15 s client) and
`(Ir_15 - Ir_5) / (req_15 - req_5)` cancels startup and shutdown. 12 connections, 128 B, pinned
core, one reactor. Raw: `2026-09-26-153-cachegrind-raw.txt`.

| build | epoll Ir/req | vs v25 | io_uring Ir/req | vs v25 |
|---|---|---|---|---|
| v25 (before §23.2) | 3341 | 1.000 | 4444 | 1.000 |
| v27 (§23.2–23.4) | 3814 | 1.142 | 5151 | 1.159 |
| v29 pooled (§23.7 first cut) | 4092 | 1.225 | 5540 | 1.247 |
| v34 unpooled | 3378 | 1.011 | 4605 | 1.036 |
| v34 pooled (default) | 3391 | 1.015 | 4561 | 1.026 |

Differences of 1–2 % between runs of the same build come from how many requests each loop round
batches, not from code. What the per-function diff showed, and what changed:

1. `ReactorStream.write` had become a real state machine (timeout bookkeeping after the call): one
   continuation allocated per write, plus allocator / memset / GC sweep work. Without timeouts it is
   now a tail call.
2. The loop read the clock every round; now only while timers or stream deadlines exist.
3. The readiness loop compared every event's fd with the lazy wake-pipe property, a synchronized
   getter; it is read once.
4. io_uring `releaseSlot` grew with the writev cleanup and stopped being inlined; the cleanup moved off
   its common path.
5. Pooling cost two pool round trips per request (release on the write drain, acquire on the next
   read; release on park, acquire on wake). Reactors now pass their thread's pool explicitly, the
   pool is a flat array with inlined acquire/release, and a buffer drained by the driver's write keeps
   its array (reads return it when they park; `Framed` returns its write buffer's after each flush).

Remaining: +1.5 % (epoll) / +2.6 % (io_uring) user-space instructions against v25, mostly the §23.2
read-side bookkeeping (io_uring multishot backpressure accounting). User-space is a minority of the
per-request cost here (the rest is syscalls), so the throughput effect is below 1 %.

## §23.2 pipelining (batched flush)

Line echo through `Framed`, pinned, 12 connections, 64 B lines, 8 rounds (`…q232…`):

| depth | epoll batched / unbatched | io_uring batched / unbatched |
|---|---|---|
| 16 requests in flight | 3.56 | 4.80 |
| 1 | 0.99 | 1.03 |

## §23.4 accept mode

x4 unpinned, 6 rounds (`…q234…`). ReusePort / Handoff throughput: epoll 1.00 (100 conns) and 0.98
(1000); io_uring 1.02 and 1.00. Jain ≥ 0.97 in both modes. No winner: Handoff stays the default.

## §23.6 Unix domain sockets vs TCP loopback

Pinned, 12 connections, 10 rounds (`…q236-unix…`):

| payload | epoll unix / tcp | io_uring unix / tcp |
|---|---|---|
| 128 B | 1.64 | 1.38 |
| 16 KiB | 1.38 | 1.20 |

## §23.7 idle memory

One reactor, 1000 connections each echoed once then idle, server RSS growth (KiB), v34:

| payload | epoll no pool | epoll pooled | io_uring no pool | io_uring pooled |
|---|---|---|---|---|
| 128 B | 5248 | 2176 | 8828 | 7804 |
| 16 KiB | 30592 | 2944 | 26240 | 9344 |

io_uring's pooled figure includes its multishot provided-buffer ring (512 × 16 KiB), which exists
with or without the pool.
