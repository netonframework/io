# neton-io

A coroutine-native, high-performance async I/O framework for Kotlin/Native, and the shared
I/O foundation for the Neton stack.

- The architecture follows the ntex-io **buffered filter** model, mapped onto Kotlin
  coroutines. `geario` (the ntex network layer extracted into one crate) is the reference.
- It backs long-lived connections/RPC and, later, HTTP — a path to dropping the Rust
  Tokio+Hyper engine and unifying the stack on Kotlin/Native.
- Performance target: the strongest Go networking libraries (gnet, netpoll); ntex/geario
  as the reference ceiling.

See [`SPEC.md`](./SPEC.md) for the full design.

## Modules

- `neton-io-bytes` — growable byte buffer (moves to native/pinned memory later)
- `neton-io-codec` — `Decoder`/`Encoder` and `LineCodec`
- `neton-io-core` — `IoStream` / `Filter` / `Io` / `Framed` / `Service` / `dispatcher` / `Readiness`
- `neton-io-testkit` — in-memory duplex driver and integration tests
- `neton-io-net` — the reactor over TCP. Readiness drivers: kqueue (Apple), epoll and poll (Linux). Completion driver: io_uring (Linux). Selectable via `NETON_IO_DRIVER`

## Design

- **Coroutine-native, not an actor framework.** The public API is `suspend`/`Flow`. Per-connection
  ownership (one coroutine, pinned to one reactor thread, share-nothing) gives the lock-free
  serialization that an actor mailbox would, without the ceremony.
- **The reactor is the scheduler.** It doubles as the `CoroutineDispatcher`; a coroutine blocked
  on I/O parks on an fd and is resumed when the poller reports readiness. The loop blocks in
  `kevent`/`epoll_wait` when idle — no busy polling.
- **Readiness and completion share one `Reactor`.** Readiness drivers (kqueue/epoll/poll) wait for
  the fd then do the syscall; the completion driver (io_uring) submits the op with its buffer and
  awaits the result. The upper layers are written against the same suspend read/write/accept.
- **TLS is a filter.** It slots in as a layer between the socket and the codec.

## Build and test

```bash
./gradlew macosArm64Test     # macOS (Apple Silicon), kqueue reactor
./gradlew linuxX64Test       # Linux, epoll reactor
./gradlew mingwX64Test       # Windows (core modules; net lands in P3)
```

Cross-compile the Linux test binary from macOS and run it elsewhere:

```bash
./gradlew :neton-io-net:linkDebugTestLinuxX64
scp neton-io-net/build/bin/linuxX64/debugTest/test.kexe <host>:/tmp/
ssh <host> /tmp/test.kexe
```

## Status

**P0** (model + in-memory driver, no cinterop) and **P1** (kqueue/epoll reactor + non-blocking TCP)
are green on macOS (kqueue) and Linux (epoll). The same `Framed`/`Service`/`dispatcher` code runs
unchanged over the in-memory driver and over real sockets.

Next (P2/P3): TLS filter and WebSocket codec; then io_uring (Linux) and IOCP (Windows), and an
HTTP layer.

## Benchmark

Raw byte echo (`echoServer`/`echoClient`), client and server on the same host, localhost,
release build, 64 B payload, single reactor. The CI `bench` job runs a short version on every push.

| Platform / driver | 1 conn | 50 conns | 200 conns |
|---|---|---|---|
| macOS kqueue (Apple Silicon) | 46,305 | 110,606 | 105,799 req/s |
| Linux epoll (2-core) | 25,192 | 90,181 | 79,451 req/s |
| Linux poll(2) (2-core) | — | 93,736 | 75,572 req/s |
| Linux io_uring (2-core) | — | 77,973 | 71,352 req/s |

poll(2) is O(n) per call, so it edges ahead at low connection counts and falls behind epoll as
the fd set grows. The read/write path is allocation-free — the socket fills and drains the
`Buffer` backing directly. The levers to peak, in order:

1. **Arm once, edge-triggered** — remove the per-round `epoll_ctl` re-arm.
2. **Multi-reactor** — one reactor per core with connection affinity.
3. **io_uring** — batched submission, completion-based, registered buffers.

```bash
# select the driver at runtime (Linux): NETON_IO_DRIVER=epoll|polling
./echoServer 0.0.0.0 9000
./echoClient 127.0.0.1 9000 50 5 64   # host port connections seconds payload
```
