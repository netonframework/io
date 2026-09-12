package neton.io.core

import neton.io.bytes.Buffer

/** Filter readiness state. */
enum class Readiness { Ready, Shutdown, Terminate }

/**
 * The coroutine-native raw byte stream at the driver boundary.
 *
 * This is the core abstraction of neton-io:
 * - the in-memory testkit driver implements it with no platform I/O;
 * - the network drivers (kqueue/epoll, later io_uring/IOCP) implement it over a real fd —
 *   `read` parks the coroutine when there is nothing to read and is resumed by the reactor
 *   on readiness. The interface is identical across both.
 *
 * Contract: `read` returns a positive count or -1 (EOF); it never returns 0 — it suspends
 * instead of reporting "no data".
 */
interface IoStream {
    /** Read available bytes into [dst]; returns the count (>0), or -1 at EOF; suspends when idle. */
    suspend fun read(dst: Buffer): Int

    /** Write all readable bytes from [src]; returns the number written. */
    suspend fun write(src: Buffer): Int

    suspend fun flush()

    fun close()
}

/**
 * A layer that wraps a lower [IoStream] and may transform bytes (decorator).
 *
 * This is where TLS lives: a future `TlsFilter` decrypts on read and encrypts on write with
 * every other layer unaware. The P0 filters are identity/demo layers proving composition.
 */
interface Filter : IoStream

/** Identity (pass-through) filter. */
class BaseFilter(private val inner: IoStream) : Filter {
    override suspend fun read(dst: Buffer): Int = inner.read(dst)
    override suspend fun write(src: Buffer): Int = inner.write(src)
    override suspend fun flush() = inner.flush()
    override fun close() = inner.close()
}
