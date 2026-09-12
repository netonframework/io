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
- `neton-io-net` — the reactor: kqueue (Apple) / epoll (Linux) with non-blocking TCP

## Design

- **Coroutine-native, not an actor framework.** The public API is `suspend`/`Flow`. Per-connection
  ownership (one coroutine, pinned to one reactor thread, share-nothing) gives the lock-free
  serialization that an actor mailbox would, without the ceremony.
- **The reactor is the scheduler.** It doubles as the `CoroutineDispatcher`; a coroutine blocked
  on I/O parks on an fd and is resumed when the poller reports readiness. The loop blocks in
  `kevent`/`epoll_wait` when idle — no busy polling.
- **Readiness now, completion later.** kqueue/epoll fit the current `suspend read/write` driver.
  A completion-oriented, buffer-feed SPI is added with io_uring/IOCP, where it pays off.
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
