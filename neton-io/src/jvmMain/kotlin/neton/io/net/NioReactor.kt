package neton.io.net

import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import neton.io.core.intResult
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

/**
 * The JVM driver: a readiness reactor over a java.nio [Selector], which is epoll on Linux and
 * Android, kqueue on macOS and the platform's poller elsewhere. It plays the part ReadinessReactor
 * plays on native — same parking, cancellation, timeout, fairness and close rules — without the
 * native-only parts: no pinning (NIO reads into a ByteBuffer view of the Buffer's array), no
 * edge-triggered interest, no speculative-read heuristics.
 *
 * Interest is one-shot, as with EPOLLONESHOT: whatever fired is removed from the key's interest
 * set, and the coroutine that waits next arms it again. Cross-thread work wakes the Selector
 * directly ([Selector.wakeup] is thread-safe and never lost: an early one makes the next select
 * return at once), so this driver has no self-pipe.
 */
@OptIn(InternalCoroutinesApi::class)
internal class NioReactor : Reactor() {

    override val driverName: String get() = "nio"

    private val selector: Selector = Selector.open()

    // Per-fd state, indexed by the Channels id, grown on demand.
    private var keys = arrayOfNulls<SelectionKey>(64)
    private var readWaiters = arrayOfNulls<Continuation<Unit>>(64)     // accept, awaitReadable
    private var writeWaiters = arrayOfNulls<Continuation<Unit>>(64)    // connect, writev, awaitWritable
    private var readConts = arrayOfNulls<Continuation<Int>>(64)
    private var readBufs = arrayOfNulls<Buffer>(64)
    private var readSizers = arrayOfNulls<ReadSizer>(64)
    private var writeConts = arrayOfNulls<Continuation<Int>>(64)
    private var writeBufs = arrayOfNulls<Buffer>(64)
    private var writeTotals = IntArray(64)
    private var readJobs = arrayOfNulls<Job>(64)
    private var writeJobs = arrayOfNulls<Job>(64)
    private var readCancelHandles = arrayOfNulls<DisposableHandle>(64)
    private var writeCancelHandles = arrayOfNulls<DisposableHandle>(64)
    // SPEC §19.5 fairness: one successful recv per connection per round; the rest wait a round.
    private var servedRound = IntArray(64)
    private var round = 1
    private var deferredFds = IntArray(64)
    private var deferredCount = 0
    private var isDeferred = BooleanArray(64)
    // A reusable NIO view per direction, rebuilt only when the Buffer's backing array changes.
    private var readArrays = arrayOfNulls<ByteArray>(64)
    private var readViews = arrayOfNulls<ByteBuffer>(64)
    private var writeArrays = arrayOfNulls<ByteArray>(64)
    private var writeViews = arrayOfNulls<ByteBuffer>(64)
    // SPEC §24 idle sweep, as ReadinessReactor: a read parked across a whole sweep interval (or when
    // the reactor goes idle) gives its pooled array back.
    private var parkEpoch = IntArray(64)
    private var sweepEpoch = 1
    private var roundsSinceSweep = 0
    private var idleArrays = false

    private fun ensureFd(fd: Int) { if (fd >= keys.size) growFd(fd) }

    private fun growFd(fd: Int) {
        var n = keys.size
        while (n <= fd) n *= 2
        keys = keys.copyOf(n)
        readWaiters = readWaiters.copyOf(n); writeWaiters = writeWaiters.copyOf(n)
        readConts = readConts.copyOf(n); readBufs = readBufs.copyOf(n); readSizers = readSizers.copyOf(n)
        writeConts = writeConts.copyOf(n); writeBufs = writeBufs.copyOf(n); writeTotals = writeTotals.copyOf(n)
        readJobs = readJobs.copyOf(n); writeJobs = writeJobs.copyOf(n)
        readCancelHandles = readCancelHandles.copyOf(n); writeCancelHandles = writeCancelHandles.copyOf(n)
        servedRound = servedRound.copyOf(n); isDeferred = isDeferred.copyOf(n)
        readArrays = readArrays.copyOf(n); readViews = readViews.copyOf(n)
        writeArrays = writeArrays.copyOf(n); writeViews = writeViews.copyOf(n)
        parkEpoch = parkEpoch.copyOf(n)
    }

    private fun sweepParked(all: Boolean) {
        idleArrays = false
        for (fd in 0 until readBufs.size) {
            val b = readBufs[fd] ?: continue
            if (!b.holdsIdleArray) continue
            if (all || parkEpoch[fd] < sweepEpoch) b.releaseIfIdle(bufferPool) else idleArrays = true
        }
        sweepEpoch++
        roundsSinceSweep = 0
    }

    override fun wakeup() { selector.wakeup() }

    // ---- interest

    private fun channel(fd: Int): SelectableChannel =
        Channels[fd] as? SelectableChannel ?: throw ClosedException("fd $fd is not open")

    private fun arm(fd: Int, ops: Int) {
        val key = keys[fd]
        if (key != null && key.isValid) {
            key.interestOps(key.interestOps() or ops)
            return
        }
        keys[fd] = channel(fd).register(selector, ops, fd)
    }

    private fun armRead(fd: Int) =
        arm(fd, if (Channels[fd] is ServerSocketChannel) SelectionKey.OP_ACCEPT else SelectionKey.OP_READ)

    /** A connect still in progress reports completion as OP_CONNECT, not OP_WRITE. */
    private fun armWrite(fd: Int) =
        arm(fd, if ((Channels[fd] as? SocketChannel)?.isConnectionPending == true) SelectionKey.OP_CONNECT else SelectionKey.OP_WRITE)

    // ---- parking and cancellation (as ReadinessReactor)

    private suspend fun waitReadable(fd: Int): Unit = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = false, cont)
        readWaiters[fd] = cont
        armRead(fd)
        COROUTINE_SUSPENDED
    }

    private suspend fun waitWritable(fd: Int): Unit = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = true, cont)
        writeWaiters[fd] = cont
        armWrite(fd)
        COROUTINE_SUSPENDED
    }

    private fun watchCancellation(fd: Int, write: Boolean, cont: Continuation<*>) {
        val job = cont.context[Job] ?: return
        if (!job.isActive) throw job.getCancellationException()
        val jobs = if (write) writeJobs else readJobs
        if (jobs[fd] === job) return
        val handles = if (write) writeCancelHandles else readCancelHandles
        handles[fd]?.dispose()
        jobs[fd] = job
        handles[fd] = job.invokeOnCompletion(onCancelling = true, invokeImmediately = false) { cause ->
            if (cause != null) postToReactor { onParkCancelled(fd, write, job) }
        }
        if (!job.isActive) { handles[fd]?.dispose(); handles[fd] = null; jobs[fd] = null; throw job.getCancellationException() }
    }

    private fun onParkCancelled(fd: Int, write: Boolean, job: Job) {
        if (fd >= readWaiters.size) return
        val waiters = if (write) writeWaiters else readWaiters
        val w = waiters[fd]
        if (w != null && w.context[Job] === job) { waiters[fd] = null; enqueueResume(w, job.getCancellationException()) }
        if (write) { if (writeConts[fd]?.context?.get(Job) === job) finishWrite(fd, 0, job.getCancellationException()) }
        else if (readConts[fd]?.context?.get(Job) === job) finishRead(fd, 0, job.getCancellationException())
    }

    override fun timeoutParked(fd: Int, reads: Boolean, writes: Boolean, cause: Throwable) {
        if (fd >= readWaiters.size) return
        if (reads) {
            readWaiters[fd]?.let { readWaiters[fd] = null; enqueueResume(it, cause) }
            if (readConts[fd] != null) finishRead(fd, 0, cause)
        }
        if (writes) {
            writeWaiters[fd]?.let { writeWaiters[fd] = null; enqueueResume(it, cause) }
            if (writeConts[fd] != null) finishWrite(fd, 0, cause)
        }
    }

    private fun forgetCancellation(fd: Int) {
        readCancelHandles[fd]?.dispose(); writeCancelHandles[fd]?.dispose()
        readCancelHandles[fd] = null; writeCancelHandles[fd] = null
        readJobs[fd] = null; writeJobs[fd] = null
    }

    // ---- data views

    private fun readView(fd: Int, array: ByteArray): ByteBuffer {
        if (readArrays[fd] !== array) { readArrays[fd] = array; readViews[fd] = ByteBuffer.wrap(array) }
        return readViews[fd]!!
    }

    private fun writeView(fd: Int, array: ByteArray): ByteBuffer {
        if (writeArrays[fd] !== array) { writeArrays[fd] = array; writeViews[fd] = ByteBuffer.wrap(array) }
        return writeViews[fd]!!
    }

    private fun socket(fd: Int): SocketChannel = Channels[fd] as? SocketChannel ?: throw ClosedException("fd $fd is not open")

    // ---- reads

    override suspend fun read(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        ensureFd(fd)
        if (readConts[fd] != null || readWaiters[fd] != null) throw IllegalStateException("concurrent read on fd $fd")
        if (servedRound[fd] != round) {
            val n = tryRecv(fd, dst, sizer)
            if (n != RECV_WAIT) return intResult(n)
        }
        return parkRead(fd, dst, sizer)
    }

    /** One read into [dst]: bytes (> 0), -1 at EOF, or [RECV_WAIT]; throws on a socket error. */
    private fun tryRecv(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        val cap = dst.reserve(sizer.readChunk(), bufferPool)     // may replace the backing array: view after
        val view = readView(fd, dst.backingArray())
        view.window(dst.writerIndex(), dst.writerIndex() + cap)
        val n = try { socket(fd).read(view) } catch (e: IOException) {
            val code = JvmErrno.record(e)
            throw IoException("read failed: ${errnoMessage(code)}", code)
        }
        stats?.let { it.reads++; if (n > 0) it.readBytes += n else if (n == 0) it.readsWouldBlock++ }
        return when {
            n > 0 -> { dst.commitWrite(n); servedRound[fd] = round; sizer.onRead(n); n }
            n < 0 -> -1
            else -> RECV_WAIT
        }
    }

    private suspend fun parkRead(fd: Int, dst: Buffer, sizer: ReadSizer): Int = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = false, cont)
        if (dst.holdsIdleArray) { parkEpoch[fd] = sweepEpoch; idleArrays = true }
        readConts[fd] = cont; readBufs[fd] = dst; readSizers[fd] = sizer
        if (servedRound[fd] == round) defer(fd) else armRead(fd)
        COROUTINE_SUSPENDED
    }

    private fun defer(fd: Int) {
        if (isDeferred[fd]) return
        if (deferredCount == deferredFds.size) deferredFds = deferredFds.copyOf(deferredCount * 2)
        deferredFds[deferredCount++] = fd
        isDeferred[fd] = true
    }

    private fun completeRead(fd: Int) {
        if (servedRound[fd] == round) { defer(fd); return }
        val n = try { tryRecv(fd, readBufs[fd]!!, readSizers[fd]!!) } catch (t: Throwable) { finishRead(fd, 0, t); return }
        if (n == RECV_WAIT) { armRead(fd); return }
        finishRead(fd, n, null)
    }

    private fun finishRead(fd: Int, n: Int, error: Throwable?) {
        val c = readConts[fd] ?: return
        readConts[fd] = null; readBufs[fd] = null; readSizers[fd] = null
        enqueueResumeInt(c, n, error)
    }

    // ---- writes

    /** One write from [src]'s readable bytes: bytes written, 0 when the socket buffer is full. */
    private fun sendSome(fd: Int, src: Buffer): Int {
        val view = writeView(fd, src.backingArray())
        view.window(src.readerIndex(), src.readerIndex() + src.readableBytes)
        val n = try { socket(fd).write(view) } catch (e: IOException) {
            val code = JvmErrno.record(e)
            throw IoException("write failed: ${errnoMessage(code)}", code)
        }
        stats?.let { it.writes++; if (n > 0) it.writeBytes += n else it.writesWouldBlock++ }
        return n
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        ensureFd(fd)
        if (writeConts[fd] != null || writeWaiters[fd] != null) throw IllegalStateException("concurrent write on fd $fd")
        var total = 0
        while (src.readableBytes > 0) {
            val n = sendSome(fd, src)
            if (n == 0) return parkWrite(fd, src, total)
            src.consumeSent(n); total += n
        }
        return intResult(total)
    }

    private suspend fun parkWrite(fd: Int, src: Buffer, sentSoFar: Int): Int = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = true, cont)
        writeConts[fd] = cont; writeBufs[fd] = src; writeTotals[fd] = sentSoFar
        armWrite(fd)
        COROUTINE_SUSPENDED
    }

    private fun completeWrite(fd: Int) {
        val src = writeBufs[fd]!!
        var total = writeTotals[fd]
        while (src.readableBytes > 0) {
            val n = try { sendSome(fd, src) } catch (t: Throwable) { finishWrite(fd, 0, t); return }
            if (n == 0) { writeTotals[fd] = total; armWrite(fd); return }
            src.consumeSent(n); total += n
        }
        finishWrite(fd, total, null)
    }

    private fun finishWrite(fd: Int, total: Int, error: Throwable?) {
        val c = writeConts[fd] ?: return
        writeConts[fd] = null; writeBufs[fd] = null
        enqueueResumeInt(c, total, error)
    }

    /** One gathering write per batch of up to [MAX_IOV] buffers; waits for writability when the socket is full. */
    override suspend fun writev(fd: Int, bufs: Array<Buffer>, count: Int): Long {
        ensureFd(fd)
        if (writeConts[fd] != null || writeWaiters[fd] != null) throw IllegalStateException("concurrent write on fd $fd")
        var total = 0L
        var i = 0
        while (i < count && bufs[i].readableBytes == 0) i++
        while (i < count) {
            val batch = minOf(MAX_IOV, count - i)
            val views = Array(batch) { k -> bufs[i + k].let { b -> ByteBuffer.wrap(b.backingArray(), b.readerIndex(), b.readableBytes) } }
            val n = try { socket(fd).write(views) } catch (e: IOException) {
                val code = JvmErrno.record(e)
                throw IoException("writev failed: ${errnoMessage(code)}", code)
            }
            stats?.let { it.writes++; if (n > 0) it.writeBytes += n else it.writesWouldBlock++ }
            if (n > 0) { total += n; i = advanceBuffers(bufs, i, count, n) } else waitWritable(fd)
        }
        return total
    }

    override suspend fun accept(listenFd: Int): Int {
        ensureFd(listenFd)
        while (true) {
            val clientFd = acceptOne(listenFd)
            if (clientFd >= 0) { ensureFd(clientFd); return clientFd }
            waitReadable(listenFd)
        }
    }

    override suspend fun awaitConnect(fd: Int) { ensureFd(fd); waitWritable(fd) }

    override suspend fun awaitReadable(fd: Int) { ensureFd(fd); waitReadable(fd) }

    override suspend fun awaitWritable(fd: Int) { ensureFd(fd); waitWritable(fd) }

    // ---- the loop

    override fun runUntil(root: Job) {
        while (true) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (readyToStop(root)) break

            var timeout = if (hasTasks() || deferredCount > 0) 0 else nextTimerMillis()
            val sweepWait = idleArrays && (timeout < 0 || timeout > IDLE_SWEEP_MS)
            if (sweepWait) timeout = IDLE_SWEEP_MS
            val selectStart = if (sweepWait) reactorNowMs() else 0L
            val n = when {
                timeout == 0 -> selector.selectNow()
                timeout < 0 -> selector.select()
                else -> selector.select(timeout.toLong())
            }
            countPoll(timeout == 0, n)
            // A Selector.wakeup() also returns 0 events; only a select that waited the whole interval was idle.
            if (sweepWait && n == 0 && reactorNowMs() - selectStart >= IDLE_SWEEP_MS) sweepParked(all = true)
            else if (++roundsSinceSweep >= SWEEP_ROUNDS && idleArrays) sweepParked(all = false)
            val selected = selector.selectedKeys()
            if (selected.isNotEmpty()) {
                val it = selected.iterator()
                while (it.hasNext()) {
                    val key = it.next()
                    it.remove()
                    val fd = key.attachment() as Int
                    val ready = try {
                        val r = key.readyOps()
                        key.interestOps(key.interestOps() and r.inv())   // one-shot
                        r
                    } catch (_: CancelledKeyException) { continue }
                    if (ready and (SelectionKey.OP_READ or SelectionKey.OP_ACCEPT) != 0) {
                        val w = readWaiters[fd]
                        if (w != null) { readWaiters[fd] = null; enqueueResume(w) }
                        if (readConts[fd] != null) completeRead(fd)
                    }
                    if (ready and (SelectionKey.OP_WRITE or SelectionKey.OP_CONNECT) != 0) {
                        val w = writeWaiters[fd]
                        if (w != null) { writeWaiters[fd] = null; enqueueResume(w) }
                        if (writeConts[fd] != null) completeWrite(fd)
                    }
                }
            }
            // New round: reads deferred in the last one get their turn now (SPEC §19.5).
            round++
            val deferred = deferredCount
            deferredCount = 0
            for (i in 0 until deferred) {
                val fd = deferredFds[i]
                isDeferred[fd] = false
                if (readConts[fd] != null) completeRead(fd)
            }
        }
    }

    override fun closeStream(fd: Int) {
        checkOwner("close")
        ensureFd(fd)
        readWaiters[fd]?.let { readWaiters[fd] = null; enqueueResume(it, ClosedException()) }
        writeWaiters[fd]?.let { writeWaiters[fd] = null; enqueueResume(it, ClosedException()) }
        if (readConts[fd] != null) finishRead(fd, 0, ClosedException())
        if (writeConts[fd] != null) finishWrite(fd, 0, ClosedException())
        forgetCancellation(fd)
        servedRound[fd] = 0
        readArrays[fd] = null; readViews[fd] = null; writeArrays[fd] = null; writeViews[fd] = null
        keys[fd]?.cancel(); keys[fd] = null
        closeFd(fd)
    }

    override fun shutdown() {
        // Wakeups stop first, so none reaches a closed Selector (SPEC §27.12).
        closeWakePipe()
        try { selector.close() } catch (_: IOException) { }
    }

    private companion object {
        /** [tryRecv]: nothing to read yet; distinct from -1, which is EOF. */
        const val RECV_WAIT = Int.MIN_VALUE
        /** Quiet time after which parked reads give their pooled arrays back. */
        const val IDLE_SWEEP_MS = 50
        /** Under load, reads parked for a whole interval of this many rounds give theirs back. */
        const val SWEEP_ROUNDS = 1024
    }
}

/**
 * Set the view to `[position, limit)`. Through [java.nio.Buffer]: ByteBuffer's own covariant
 * limit/position overrides are Java 9, and code compiled against them fails with NoSuchMethodError
 * on Java 8 and on older Android.
 */
private fun ByteBuffer.window(position: Int, limit: Int) {
    val b: java.nio.Buffer = this
    b.limit(limit)
    b.position(position)
}
