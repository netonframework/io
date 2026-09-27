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
 * - [IoStream.shutdownOutput] on one end: the peer reads what was sent, then EOF.
 * - [IoStream.close] on one end: the peer reads what was sent, then EOF, and its writes fail with
 *   [IoException]; operations parked on the closed end fail with [ClosedException].
 * - Unlike a socket stream, either end may be used from any thread (a short lock guards each direction).
 */
fun memoryStreamPair(capacity: Int = 64 * 1024): Pair<IoStream, IoStream> {
    require(capacity > 0) { "capacity must be positive" }
    val ab = MemoryPipe(capacity)
    val ba = MemoryPipe(capacity)
    return MemoryStream(inbound = ba, outbound = ab) to MemoryStream(inbound = ab, outbound = ba)
}

/** One direction: a bounded byte queue with one parked reader and one parked writer at most. */
private class MemoryPipe(val capacity: Int) {
    private val lock = AtomicInt(0)
    val data = Buffer()
    /** The writing end shut down or closed: EOF once [data] is drained. */
    var writerDone = false
    /** The reading end closed: writes fail. */
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

private class MemoryStream(private val inbound: MemoryPipe, private val outbound: MemoryPipe) : IoStream {
    private val closed = AtomicInt(0)

    private fun checkOpen() { if (closed.load() != 0) throw ClosedException() }

    override suspend fun read(dst: Buffer): Int {
        while (true) {
            checkOpen()
            var wait: CompletableDeferred<Unit>? = null
            var wakeWriter: CompletableDeferred<Unit>? = null
            val n = inbound.locked {
                val p = inbound
                when {
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
            if (n != 0) return n
            wait!!.await()
        }
    }

    override suspend fun write(src: Buffer): Int {
        val total = src.readableBytes
        while (src.readableBytes > 0) {
            checkOpen()
            var wait: CompletableDeferred<Unit>? = null
            var wakeReader: CompletableDeferred<Unit>? = null
            val gone = outbound.locked {
                val p = outbound
                if (p.readerGone) return@locked true
                val space = p.capacity - p.data.readableBytes
                if (space > 0) {
                    val n = minOf(space, src.readableBytes)
                    p.data.writeBytes(src.backingArray(), src.readerIndex(), n)
                    src.skip(n)
                    wakeReader = p.readWaiter; p.readWaiter = null
                } else {
                    wait = CompletableDeferred<Unit>().also { p.writeWaiter = it }
                }
                false
            }
            wakeReader?.complete(Unit)
            if (gone) throw IoException("write failed: peer closed (EPIPE)", 32)
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
        outbound.locked { outbound.writerDone = true }
        inbound.locked { inbound.readerGone = true; inbound.data.clear() }
        // Parked peers see EOF / a failed write; this end's own parked operations see ClosedException.
        outbound.wakeAll(); inbound.wakeAll()
    }
}
