@file:OptIn(ExperimentalAtomicApi::class)

package neton.io.core

import kotlinx.coroutines.CompletableDeferred
import neton.io.bytes.Buffer
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * A connected pair of in-memory [IoStream]s (SPEC §27.4): bytes written on one end are read on the
 * other, with no platform I/O. For testing protocol code (codecs, `Framed`, services, msgtrans)
 * without sockets, as geario's `IoTest` does.
 *
 * - Each direction holds at most [capacity] bytes: a write beyond that suspends until the peer reads.
 * - [IoStream.shutdownOutput] on one end: the peer reads what was sent, then EOF; later writes on
 *   this end (and one parked when it happens) fail with [IoException]; this end can still read.
 * - [IoStream.close] on one end: the peer reads what was sent, then EOF, and its writes fail with
 *   [IoException]; operations on the closed end, parked or later, fail with [ClosedException].
 * - Unlike a socket stream, either end may be used from any thread (a short lock guards each direction).
 *   As on a socket stream, each end allows one read and one write at a time: a second concurrent
 *   read (or write) on the same end throws [IllegalStateException] (SPEC §27.8).
 */
fun memoryStreamPair(capacity: Int = 64 * 1024): Pair<IoStream, IoStream> = memoryStreamPairWithHook(capacity, null)

/**
 * Tests: [beforeLock] runs in `read` / `write` after the stream's open check and before the pipe
 * lock is taken, so a test can close the stream exactly there (the window a concurrent close hits).
 */
internal fun memoryStreamPairWithHook(capacity: Int, beforeLock: (() -> Unit)?): Pair<IoStream, IoStream> {
    require(capacity > 0) { "capacity must be positive" }
    val ab = MemoryPipe(capacity)
    val ba = MemoryPipe(capacity)
    return MemoryStream(inbound = ba, outbound = ab, beforeLock) to MemoryStream(inbound = ab, outbound = ba, beforeLock)
}

/** One direction: a bounded byte queue with one parked reader and one parked writer at most. */
private class MemoryPipe(val capacity: Int) {
    private val lock = AtomicInt(0)
    val data = Buffer()
    /** The writing end shut down its output: EOF once [data] is drained; further writes fail. */
    var writerDone = false
    /** The writing end closed (implies [writerDone]); its own writes fail with [ClosedException]. */
    var writerClosed = false
    /** The reading end closed: its reads fail with [ClosedException], the peer's writes fail. */
    var readerGone = false
    var readWaiter: CompletableDeferred<Unit>? = null
    var writeWaiter: CompletableDeferred<Unit>? = null

    inline fun <T> locked(block: () -> T): T {
        while (!lock.compareAndSet(0, 1)) { /* critical sections are a copy of at most `capacity` bytes */ }
        try { return block() } finally { lock.store(0) }
    }

    /** Wake whoever is parked on this pipe (outside the lock). */
    fun wakeAll() {
        val (r, w) = locked { val r = readWaiter; val w = writeWaiter; readWaiter = null; writeWaiter = null; r to w }
        r?.complete(Unit); w?.complete(Unit)
    }
}

/**
 * Every state change (close, shutdown, data) and every waiter registration happens under the pipe's
 * lock, and each end's own closed state is checked there too: a close that runs after the fast
 * open check but before a waiter is registered is seen under the lock, never missed.
 */
private class MemoryStream(
    private val inbound: MemoryPipe,
    private val outbound: MemoryPipe,
    private val beforeLock: (() -> Unit)?,
) : IoStream {
    private val closed = AtomicInt(0)
    // One parked reader / writer per direction (SPEC §27.8): a second one would take the first's
    // waiter slot and leave it unwoken, so it is refused instead.
    private val reading = AtomicInt(0)
    private val writing = AtomicInt(0)

    private fun checkOpen() { if (closed.load() != 0) throw ClosedException() }

    override suspend fun read(dst: Buffer): Int {
        check(reading.compareAndSet(0, 1)) { "concurrent read on one end of a memory stream" }
        try { return readOne(dst) } finally { reading.store(0) }
    }

    private suspend fun readOne(dst: Buffer): Int {
        while (true) {
            checkOpen()
            beforeLock?.invoke()
            var wait: CompletableDeferred<Unit>? = null
            var wakeWriter: CompletableDeferred<Unit>? = null
            val n = inbound.locked {
                val p = inbound
                when {
                    p.readerGone -> CLOSED
                    p.data.readableBytes > 0 -> {
                        val n = p.data.readableBytes
                        dst.writeBytes(p.data.backingArray(), p.data.readerIndex(), n)
                        p.data.skip(n)
                        p.data.discardReadBytes()
                        wakeWriter = p.writeWaiter; p.writeWaiter = null
                        n
                    }
                    p.writerDone -> -1
                    else -> { wait = CompletableDeferred<Unit>().also { p.readWaiter = it }; 0 }
                }
            }
            wakeWriter?.complete(Unit)
            if (n == CLOSED) throw ClosedException()
            if (n != 0) return n
            wait!!.await()
        }
    }

    override suspend fun write(src: Buffer): Int {
        check(writing.compareAndSet(0, 1)) { "concurrent write on one end of a memory stream" }
        try { return writeAll(src) } finally { writing.store(0) }
    }

    private suspend fun writeAll(src: Buffer): Int {
        val total = src.readableBytes
        while (src.readableBytes > 0) {
            checkOpen()
            beforeLock?.invoke()
            var wait: CompletableDeferred<Unit>? = null
            var wakeReader: CompletableDeferred<Unit>? = null
            val failure = outbound.locked {
                val p = outbound
                when {
                    p.writerClosed -> return@locked ClosedException()
                    p.writerDone -> return@locked IoException("write failed: output shut down")
                    p.readerGone -> return@locked IoException("write failed: peer closed (EPIPE)", 32)
                }
                val space = p.capacity - p.data.readableBytes
                if (space > 0) {
                    val n = minOf(space, src.readableBytes)
                    p.data.writeBytes(src.backingArray(), src.readerIndex(), n)
                    src.skip(n)
                    wakeReader = p.readWaiter; p.readWaiter = null
                } else {
                    wait = CompletableDeferred<Unit>().also { p.writeWaiter = it }
                }
                null
            }
            wakeReader?.complete(Unit)
            if (failure != null) throw failure
            wait?.await()
        }
        return total
    }

    override suspend fun flush() {}

    override suspend fun shutdownOutput() {
        outbound.locked { outbound.writerDone = true }
        outbound.wakeAll()
    }

    override fun close() {
        if (!closed.compareAndSet(0, 1)) return
        outbound.locked { outbound.writerDone = true; outbound.writerClosed = true }
        inbound.locked { inbound.readerGone = true; inbound.data.clear() }
        // Parked peers see EOF / a failed write; this end's own parked operations see ClosedException.
        outbound.wakeAll(); inbound.wakeAll()
    }
}

/** [MemoryStream.read]'s marker for "this end was closed" (a real count is > 0, EOF is -1, wait is 0). */
private const val CLOSED = Int.MIN_VALUE
