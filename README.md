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

## Artifact

One artifact, `com.netonstream:io`, the way tokio is one crate. It is a new coordinate with its own version line
starting at 0.1.0; the older `com.netonstream:neton-io:0.1.0` is a different, earlier artifact. Packages keep the layering:

- `neton.io.bytes` — growable byte buffer (moves to native/pinned memory later)
- `neton.io.codec` — `Decoder`/`Encoder` and `LineCodec`
- `neton.io.core` — `IoStream` / `Filter` / `Io` / `Framed` / `Service` / `dispatcher` / `Readiness`
- `neton.io.net` — the reactor over TCP. Readiness drivers: kqueue (Apple), epoll and poll (Linux), java.nio `Selector` (JVM). Completion driver: io_uring (Linux). Selectable via `NETON_IO_DRIVER`

```kotlin
dependencies { implementation("com.netonstream:io:0.2.0") }
```

Targets: macOS, iOS, Linux (x64/arm64), Android native, Windows (mingwX64, IOCP and WSAPoll drivers) and the
JVM. The native artifacts are klibs, so a consumer compiles with the release's Kotlin version (2.4.0).

**JVM** (2026-10-03). The reactor core, timers, cross-thread dispatch, lifecycle, TCP layer, `ReactorGroup`,
`serveTcp` and the public API are the same code as on native; only the driver differs: `NioReactor`, a
readiness driver over `java.nio.channels.Selector` (epoll on Linux and Android, kqueue on macOS), with one-shot
interest and the same parking, cancellation, timeout, fairness, idle-sweep and close rules as the native
readiness driver. It uses no native code, bytecode 1.8 and only APIs Android has had since API 21, so an Android
app or library can depend on it. Not available on the JVM: UDP and Unix sockets, signal handling
(`shutdownOnSignal`, `serveTcp(shutdownOnSignals = true)` throws — the VM owns the signals), thread pinning, the
GC tuning hooks, keepalive timings (keepalive on/off only; the JDK has no portable API for idle / interval /
probes), and SO_REUSEPORT load balancing (`AcceptMode.ReusePort` falls back to hand-off). The native readiness
driver is untouched by the port: it stays in nativeMain with its pinned zero-copy data path.

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
```

Run the full Linux suite on a Linux host (needed for the io_uring/epoll/poll drivers), pinning the
driver:

```bash
# on a Linux host with JDK 17 (the macOS box cannot run the Linux reactors)
NETON_IO_DRIVER=iouring ./gradlew :io:linuxX64Test                          # io_uring
NETON_IO_DRIVER=epoll   ./gradlew :io:linuxX64Test                          # epoll
NETON_IO_DRIVER=iouring NETON_IO_URING_DEPTH=8 ./gradlew :io:linuxX64Test   # full-SQ path
```

## Status

**P0** (model + in-memory driver) and **P1** (kqueue/epoll reactor + non-blocking TCP) are green;
the io_uring completion driver, the threading contract (cross-thread dispatch + wakeup, owner
checks), reactor timers, and the buffer-lifecycle / close / cancel paths are implemented.

Verified (2026-09-13), reproducible: `neton-io` `linuxX64Test` (13 cases) passes with 0
failures on a Rocky Linux 9.8 / kernel 5.14 / x86_64 host under **io_uring, epoll, poll(2), and
io_uring at SQ depth 8** (Kotlin 2.4.0, Gradle 8.14.2, JDK 17); macOS (kqueue) passes the same
cases. This is test-case pass, not a guarantee that every error path, scalability, tail latency or
memory behaviour is covered. Windows: core modules compile; the net driver (IOCP) is not built.

JVM (2026-10-03): 116 cases pass on JDK 17 (`./gradlew :io:jvmTest`), the portable part of the suite moved to
commonTest — TCP echo, timeouts, writev, fairness, idle reads, admission, multi-reactor groups, cross-thread
dispatch and cancellation, posts racing a reactor's close — and the same cases still pass on macOS and the iOS
simulator (154 each). The cross-thread cases also passed 40 rounds under full CPU contention.

Next (P2/P3): TLS filter and WebSocket codec; then io_uring optimization (batching, registered
buffers, multi-reactor) and IOCP, and an HTTP layer.

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

## License

[Apache License 2.0](LICENSE).
