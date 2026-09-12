package neton.io.core

import neton.io.bytes.Buffer

/** Filter readiness state. */
enum class Readiness { Ready, Shutdown, Terminate }

/** An I/O operation failed (socket error, or the stream was closed while an operation was parked). */
open class IoException(message: String, val errno: Int = 0) : Exception(message)

/** The stream was closed (by [IoStream.close]) while an operation was parked on it, or used after close. */
class ClosedException(message: String = "stream closed") : IoException(message)

/**
 * The coroutine-native raw byte stream at the driver boundary.
 *
 * This is the core abstraction of neton-io:
 * - the in-memory testkit driver implements it with no platform I/O;
 * - the network drivers (kqueue/epoll/poll, io_uring) implement it over a real fd —
 *   `read` parks the coroutine when there is nothing to read and is resumed by the reactor
 *   on readiness or completion. The interface is identical across both.
 *
 * Contract:
 * - `read` returns a positive count or -1 at EOF; it never returns 0 (it suspends instead).
 *   A socket error throws [IoException]; EOF and error are never conflated.
 * - `write` writes *all* readable bytes of [src] (suspending as needed) and returns the count;
 *   a socket error throws [IoException] after consuming whatever was accepted, so the caller
 *   can see how much of [src] is still unsent.
 * - `close` is idempotent. It must be called on the reactor thread that owns the stream. Any
 *   coroutine parked in `read`/`write` on the stream is resumed with [ClosedException]; a call
 *   after close throws [ClosedException]. A buffer handed to a parked operation is released only
 *   when the driver no longer needs it (completion drivers: when the kernel completes or cancels
 *   the operation), never earlier.
 * - Ownership: a stream belongs to the reactor that created it and must be used from its
 *   coroutines only; it is not thread-safe.
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
