package neton.io.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import neton.io.codec.Decoder
import neton.io.codec.Encoder
import kotlin.time.TimeSource

/**
 * Slow-sender protection for one frame (SPEC §23.2, like ntex's `frame_read_rate`): once a frame's
 * first byte has arrived, it must make progress within [timeoutMillis]; every [rateBytes] received
 * extends the deadline by [timeoutMillis] again, but never beyond [maxTimeoutMillis] after the
 * frame started. Violations throw [TimeoutException].
 */
class FrameReadRate(val timeoutMillis: Long, val maxTimeoutMillis: Long, val rateBytes: Int) {
    init { require(timeoutMillis > 0 && maxTimeoutMillis >= timeoutMillis && rateBytes > 0) }
}

/**
 * Sends and receives codec-defined frames over an [Io].
 *
 * Read side: [incoming] is a `Flow` that decodes as much as the read buffer allows, reads more when a
 * frame is incomplete, and ends at EOF. Write side: [feed] encodes into the write buffer and flushes
 * by itself above [highWatermark]; [flush] writes what is buffered (suspending while the socket is
 * full — that is the backpressure); [send] is feed + flush. [serveLoop] answers every request already
 * received before flushing once, so pipelined requests do not cost a syscall each (SPEC §23.2).
 *
 * For throughput prefer [serveLoop]: it allocates nothing per request of its own (SPEC §24).
 * [incoming] is a Flow, and each `emit` resumes the collector's suspend lambda, which allocates.
 */
class Framed<In, Out>(
    @PublishedApi internal val io: Io,
    @PublishedApi internal val decoder: Decoder<In>,
    @PublishedApi internal val encoder: Encoder<Out>,
    /** Buffered output above this many bytes is flushed by [feed] itself. */
    @PublishedApi internal val highWatermark: Int = 64 * 1024,
    /** Optional per-frame read rate; when set, Framed manages the stream's read timeout. */
    @PublishedApi internal val readRate: FrameReadRate? = null,
) {
    /** Frame stream: read then decode; read more when a frame is incomplete; end at EOF. */
    fun incoming(): Flow<In> = flow {
        val buf = io.readBuf
        val rate = FrameRateTracker(readRate)
        while (true) {
            var item = decoder.decode(buf)
            while (item != null) {
                rate.frameDone()
                emit(item)
                item = decoder.decode(buf)
            }
            buf.discardReadBytes()
            if (!readMore(buf, rate)) break // EOF
        }
    }

    /** Encode [item] into the write buffer; flush once the buffer passes [highWatermark]. */
    suspend fun feed(item: Out) = feedOut(item)

    /** Write everything buffered by [feed]. */
    suspend fun flush() = writeOut()

    /**
     * Send one frame: encode, write, flush. Inline (SPEC §24): inside the caller's loop it adds no
     * coroutine frame, so sending allocates nothing.
     */
    suspend inline fun send(item: Out) {
        feedOut(item)
        writeOut()
    }

    /**
     * Pull-style read loop (SPEC §24): call [onFrame] for every frame until EOF. Inline, so the loop
     * and [onFrame] run in the caller's own coroutine frame — unlike [incoming], whose Flow `emit`
     * resumes a collector lambda (an allocation per frame).
     */
    suspend inline fun receiveEach(crossinline onFrame: suspend (In) -> Unit) {
        val buf = io.readBuf
        val rate = FrameRateTracker(readRate)
        while (true) {
            var item = decoder.decode(buf)
            while (item != null) {
                rate.frameDone()
                onFrame(item)
                item = decoder.decode(buf)
            }
            buf.discardReadBytes()
            if (!readMore(buf, rate)) break // EOF
        }
    }

    // Inline bodies (SPEC §24): called from serveLoop's own loop they add no coroutine frame, so a
    // request/response costs no allocation here. A separate suspend function would allocate its
    // continuation on every call.
    @PublishedApi internal suspend inline fun feedOut(item: Out) {
        encoder.encode(item, io.writeBuf)
        if (io.writeBuf.readableBytes >= highWatermark) writeOut()
    }

    @PublishedApi internal suspend inline fun writeOut() {
        val out = io.writeBuf
        if (out.readableBytes > 0) {
            io.stream.write(out)
            io.stream.flush()
        }
        out.clear()
    }

    /**
     * Request/response loop until EOF: handle every request already in the read buffer, flush the
     * responses once, then read more. Order is preserved; a lone request is flushed at once.
     */
    suspend fun serveLoop(handler: suspend (In) -> Out) {
        val buf = io.readBuf
        val rate = FrameRateTracker(readRate)
        while (true) {
            var item = decoder.decode(buf)
            while (item != null) {
                rate.frameDone()
                feedOut(handler(item))
                item = decoder.decode(buf)
            }
            writeOut()
            buf.discardReadBytes()
            if (!readMore(buf, rate)) break
        }
    }

    /** One read, with the frame read rate applied to it; false at EOF. Inline: no frame of its own. */
    @PublishedApi internal suspend inline fun readMore(buf: neton.io.bytes.Buffer, rate: FrameRateTracker): Boolean {
        if (readRate != null) io.stream.setReadTimeout(rate.timeoutForNextRead(partial = buf.readableBytes > 0))
        val n = io.stream.read(buf)
        if (n < 0) return false
        rate.received(n)
        return true
    }

    /** Deadline bookkeeping for [FrameReadRate]; inert when the rate is null. */
    @PublishedApi internal class FrameRateTracker(private val rate: FrameReadRate?) {
        private val clock = TimeSource.Monotonic
        private var frameStart: TimeSource.Monotonic.ValueTimeMark? = null
        private var deadlineMs = 0L          // relative to frameStart
        private var sinceExtend = 0

        fun frameDone() { frameStart = null }

        fun received(n: Int) {
            val r = rate ?: return
            val start = frameStart ?: return
            sinceExtend += n
            if (sinceExtend >= r.rateBytes) {
                sinceExtend = 0
                val now = start.elapsedNow().inWholeMilliseconds
                deadlineMs = minOf(now + r.timeoutMillis, r.maxTimeoutMillis)
            }
        }

        /** Read timeout for the next read: none between frames; the remaining frame budget mid-frame. */
        fun timeoutForNextRead(partial: Boolean): Long {
            val r = rate ?: return 0
            if (!partial) { frameStart = null; return 0 }
            val start = frameStart ?: clock.markNow().also { frameStart = it; deadlineMs = r.timeoutMillis; sinceExtend = 0 }
            val left = deadlineMs - start.elapsedNow().inWholeMilliseconds
            if (left <= 0) throw TimeoutException("frame not received in time (read rate)")
            return left
        }
    }
}
