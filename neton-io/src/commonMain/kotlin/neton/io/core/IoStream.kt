package neton.io.core

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes

/** Filter readiness state. */
enum class Readiness { Ready, Shutdown, Terminate }

/** An I/O operation failed (socket error, or the stream was closed while an operation was parked). */
open class IoException(message: String, val errno: Int = 0) : Exception(message)

/** The stream was closed (by [IoStream.close]) while an operation was parked on it, or used after close. */
class ClosedException(message: String = "stream closed") : IoException(message)

/** A read, write, idle or frame deadline passed (SPEC §23.2). */
class TimeoutException(message: String) : IoException(message)

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

    /**
     * Write all readable bytes of `buffers[0 until count]`, in order, as one vectored write where
     * the driver supports it (SPEC §23.3); returns the bytes written. Each buffer is advanced by what
     * it contributed, so after an exception the caller can see what is still unsent. The default
     * writes the buffers one by one.
     */
    suspend fun writev(buffers: Array<Buffer>, count: Int = buffers.size): Long {
        var total = 0L
        for (i in 0 until count) if (buffers[i].readableBytes > 0) total += write(buffers[i])
        return total
    }

    /**
     * Half-close (SPEC §23.3): no more writes from this side; the peer reads EOF once everything
     * written so far has arrived. This side can still read. The default does nothing.
     */
    suspend fun shutdownOutput() {}

    /**
     * Timeouts (SPEC §23.2), in milliseconds; 0 disables. [readTimeoutMillis]: a read parked longer
     * throws [TimeoutException] (the stream stays usable). [writeTimeoutMillis]: likewise for a write
     * waiting on a full socket. [idleTimeoutMillis]: no successful read or write for that long closes
     * the stream; parked operations get [TimeoutException]. The default does nothing.
     */
    fun setTimeouts(readTimeoutMillis: Long = 0, writeTimeoutMillis: Long = 0, idleTimeoutMillis: Long = 0) {}

    /** Change only the read timeout (used per read by [Framed]'s frame read rate). */
    fun setReadTimeout(millis: Long) {}
}

/**
 * Close after a graceful half-close (SPEC §23.2): stop writing, let the peer finish (discarding what
 * it still sends) until it closes or [timeoutMillis] passes, then close. Never throws.
 */
suspend fun IoStream.closeGracefully(timeoutMillis: Long) {
    val start = kotlin.time.TimeSource.Monotonic.markNow()
    try {
        shutdownOutput()
        val sink = Buffer(1024)
        while (true) {
            val left = timeoutMillis - start.elapsedNow().inWholeMilliseconds
            if (left <= 0) break
            setReadTimeout(left)
            sink.clear()
            if (read(sink) < 0) break
        }
    } catch (_: IoException) {
    } finally {
        close()
    }
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
    override suspend fun writev(buffers: Array<Buffer>, count: Int): Long = inner.writev(buffers, count)
    override suspend fun shutdownOutput() = inner.shutdownOutput()
    override fun setTimeouts(readTimeoutMillis: Long, writeTimeoutMillis: Long, idleTimeoutMillis: Long) =
        inner.setTimeouts(readTimeoutMillis, writeTimeoutMillis, idleTimeoutMillis)
    override fun setReadTimeout(millis: Long) = inner.setReadTimeout(millis)
}

/**
 * Write [slices] in order with vectored sends, without copying them (SPEC §23.7): each slice's
 * array is sent in place. Returns the bytes written (all of them, unless an exception is thrown).
 */
suspend fun IoStream.writev(slices: List<Bytes>): Long {
    if (slices.isEmpty()) return 0
    return writev(Array(slices.size) { Buffer.wrap(slices[it]) }, slices.size)
}
