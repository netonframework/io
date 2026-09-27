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
 * Contract (SPEC §28.6; the `io-testkit` conformance suite checks it):
 * - `read` returns a positive count, appending that many bytes to `dst` (what `dst` held is kept),
 *   or -1 at EOF; it never returns 0 (it suspends instead). A socket error throws [IoException]
 *   with `dst` unchanged; EOF and error are never conflated. A peer's orderly close (FIN) is EOF
 *   once the data before it has been read; a reset (RST) is an [IoException].
 * - `write` writes *all* readable bytes of `src` (suspending as needed), returns the count and
 *   leaves `src` empty; a socket error throws [IoException] after advancing `src` by what the kernel
 *   accepted, so the caller can see how much is still unsent.
 * - Cancellation or a timeout of a parked operation, and closing the stream under it, never roll
 *   back: bytes already received are in `dst`, `src` is advanced by what was sent. Every exit
 *   happens only once the driver (and the kernel, for completion drivers) no longer touches the
 *   buffer, so it can be reused at once. Each operation ends exactly once: a count, [IoException],
 *   [ClosedException], `CancellationException` or [TimeoutException].
 * - `close` is idempotent. Parked operations end with [ClosedException] (on completion drivers
 *   once the kernel has given the operation back); later calls throw [ClosedException].
 * - At most one read and one write at a time (full duplex); a second one in the same direction
 *   throws [IllegalStateException] instead of queueing or replacing the first.
 * - Threads: by default a stream belongs to the reactor that created it and must be used on its
 *   thread; one declaring [StreamCapability.AnyThread] may be used from any thread (the rule above
 *   still holds).
 * - Optional operations are declared in [capabilities]; calling one that is not declared throws
 *   [UnsupportedOperationException] (a timeout of 0, meaning "none", is always accepted).
 */
interface IoStream {
    /** Optional operations this stream supports (SPEC §28.6). */
    val capabilities: Set<StreamCapability> get() = emptySet()

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
     * written so far has arrived. This side can still read. Needs [StreamCapability.HalfClose].
     */
    suspend fun shutdownOutput() { throw UnsupportedOperationException("shutdownOutput: this stream does not declare HalfClose") }

    /**
     * Timeouts (SPEC §23.2), in milliseconds; 0 disables. [readTimeoutMillis]: a read parked longer
     * throws [TimeoutException] (the stream stays usable). [writeTimeoutMillis]: likewise for a write
     * waiting on a full socket. [idleTimeoutMillis]: no successful read or write for that long closes
     * the stream; parked operations get [TimeoutException]. Each non-zero value needs its
     * capability ([StreamCapability.ReadTimeout], [StreamCapability.WriteTimeout],
     * [StreamCapability.IdleTimeout]); 0 is always accepted.
     */
    fun setTimeouts(readTimeoutMillis: Long = 0, writeTimeoutMillis: Long = 0, idleTimeoutMillis: Long = 0) {
        requireTimeoutCapabilities(this, readTimeoutMillis, writeTimeoutMillis, idleTimeoutMillis)
    }

    /** Change only the read timeout (used per read by [Framed]'s frame read rate). */
    fun setReadTimeout(millis: Long) { requireTimeoutCapabilities(this, millis, 0, 0) }
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

/** Optional [IoStream] operations (SPEC §28.6). */
enum class StreamCapability {
    /** [IoStream.shutdownOutput]. */
    HalfClose,
    /** A non-zero read timeout. */
    ReadTimeout,
    /** A non-zero write timeout. */
    WriteTimeout,
    /** A non-zero idle timeout. */
    IdleTimeout,
    /** Usable from any thread (still one read and one write at a time). */
    AnyThread,
    /** After a cancelled or timed-out operation the stream stays usable and loses no data. */
    ResumableAfterCancel,
}

/** Throws [UnsupportedOperationException] for a non-zero timeout the stream does not declare. */
fun requireTimeoutCapabilities(stream: IoStream, read: Long, write: Long, idle: Long) {
    val caps = stream.capabilities
    if (read != 0L && StreamCapability.ReadTimeout !in caps) throw UnsupportedOperationException("read timeout: this stream does not declare ReadTimeout")
    if (write != 0L && StreamCapability.WriteTimeout !in caps) throw UnsupportedOperationException("write timeout: this stream does not declare WriteTimeout")
    if (idle != 0L && StreamCapability.IdleTimeout !in caps) throw UnsupportedOperationException("idle timeout: this stream does not declare IdleTimeout")
}

/**
 * A layer that wraps a lower [IoStream] and may transform bytes (decorator). A transforming
 * layer (TLS, ...) declares its own [IoStream.capabilities] item by item; only a transparent one
 * such as [BaseFilter] passes the inner stream's through.
 *
 * This is where TLS lives: a future `TlsFilter` decrypts on read and encrypts on write with
 * every other layer unaware. The P0 filters are identity/demo layers proving composition.
 */
interface Filter : IoStream

/** Identity (pass-through) filter. */
class BaseFilter(private val inner: IoStream) : Filter {
    override val capabilities: Set<StreamCapability> get() = inner.capabilities
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
