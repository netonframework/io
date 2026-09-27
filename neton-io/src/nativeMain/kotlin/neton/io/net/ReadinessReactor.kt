package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.pin
import kotlinx.cinterop.toKString
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

/**
 * Readiness reactor backed by a [Poller] (kqueue/epoll/poll). A read waits for the fd to be
 * readable and then does the recv; the reactor blocks in the poller when idle (no busy poll).
 */
@OptIn(ExperimentalForeignApi::class, InternalCoroutinesApi::class)
internal class ReadinessReactor(private val poller: Poller) : Reactor() {

    override val driverName: String get() = poller.name

    // Per-fd state, indexed by fd (SPEC §17c: no boxed keys, no hashing, nothing allocated per
    // request). fds are small dense ints; the arrays grow on demand and are never shrunk.
    // SPEC §19.3: parked coroutines are stored as their own raw continuations — no
    // CancellableContinuation, no parent handle per park. Every path that resumes one (readiness,
    // cancellation, close) takes it out of its slot first, so none can be resumed twice.
    private var readWaiters = arrayOfNulls<Continuation<Unit>>(64)     // accept (listeners)
    private var writeWaiters = arrayOfNulls<Continuation<Unit>>(64)    // connect, writev
    // SPEC §24: a parked read / write is completed by the reactor itself. It stores the caller's
    // continuation (the tail-called chain allocates none of its own) with the buffer, and on
    // readiness does the recv / the remaining sends and resumes with the result.
    private var readConts = arrayOfNulls<Continuation<Int>>(64)
    private var readBufs = arrayOfNulls<Buffer>(64)
    private var readSizers = arrayOfNulls<ReadSizer>(64)
    private var writeConts = arrayOfNulls<Continuation<Int>>(64)
    private var writeBufs = arrayOfNulls<Buffer>(64)
    private var writeTotals = IntArray(64)
    // Cancellation is watched once per (fd, direction, coroutine Job), not once per park: the
    // handle is reused for every park by the same coroutine and disposed on close or when another
    // coroutine takes over that side of the stream.
    // SPEC §19.5 fairness: the poll round in which each fd last had a successful recv, and the
    // readers deferred to the next round because they already had theirs in this one.
    private var servedRound = IntArray(64)
    private var round = 1
    private var deferredFds = IntArray(64)
    private var deferredCount = 0
    private var isDeferred = BooleanArray(64)
    private var readJobs = arrayOfNulls<Job>(64)
    private var writeJobs = arrayOfNulls<Job>(64)
    private var readCancelHandles = arrayOfNulls<DisposableHandle>(64)
    private var writeCancelHandles = arrayOfNulls<DisposableHandle>(64)
    // SPEC §17: persistent edge-triggered read interest, and "an edge arrived with nobody parked".
    // A read on a persistent fd recv()s until EAGAIN, then clears the flag and parks; it never
    // recv()s speculatively.
    private var persistent = BooleanArray(64)
    private var readyRead = BooleanArray(64)
    // One pin per fd for the buffer last used on it: pinning allocates a StableRef, and the
    // buffer of a connection is the same object on every call. Replaced when the array changes
    // (Buffer growth), released on close.
    // Separate slots for the read and the write side: Framed reads into one buffer and writes from
    // another, and a single slot re-pinned (allocated) twice per request (SPEC §24).
    private var pinnedArrays = arrayOfNulls<ByteArray>(64)
    private var pins = arrayOfNulls<Pinned<ByteArray>>(64)
    private var wPinnedArrays = arrayOfNulls<ByteArray>(64)
    private var wPins = arrayOfNulls<Pinned<ByteArray>>(64)
    // SPEC §24 idle sweep: the sweep epoch in which each fd's read parked. A buffer parked across a
    // whole sweep interval (or when the reactor goes idle) gives its pooled array back.
    private var parkEpoch = IntArray(64)
    private var sweepEpoch = 1
    private var roundsSinceSweep = 0
    private var idleArrays = false          // some parked read may hold an idle pooled array
    // The last event for the fd carried EOF / hang-up: its FIN may already be in, with no edge to follow.
    private var peerClosed = BooleanArray(64)
    // Apple: SO_NOSIGPIPE could not be set (peer gone before accept); a send would raise SIGPIPE.
    private var sendsFail = BooleanArray(64)
    // SPEC §24.5 short-read rule. After a short recv the next recv on an edge-triggered fd is
    // speculative: it may find the peer's next request already there, or return EAGAIN. The reactor
    // (not each fd: a mix let speculating connections jump the edge queue, p99 +60 % and Jain 0.87 at
    // 1000 connections) watches the last 256 speculative recvs and stops speculating while more than
    // 90 % miss, still sampling every 16th short read. Never after a hang-up event: EOF must still be
    // read. NETON_IO_SHORT_READ: 0 never skip, 1 always skip, default auto.
    private val shortReadMode: Int = when (platform.posix.getenv("NETON_IO_SHORT_READ")?.toKString()) {
        "0" -> SHORT_OFF; "1" -> SHORT_ALWAYS; else -> SHORT_AUTO
    }
    private var specPending = BooleanArray(64)   // the next recv on the fd is speculative
    private var skipSpeculation = false
    private var specSeen = 0
    private var specMissed = 0
    private var shortCount = 0

    private fun ensureFd(fd: Int) {
        if (fd < persistent.size) return
        var n = persistent.size
        while (n <= fd) n *= 2
        readWaiters = readWaiters.copyOf(n); writeWaiters = writeWaiters.copyOf(n)
        readConts = readConts.copyOf(n); readBufs = readBufs.copyOf(n); readSizers = readSizers.copyOf(n)
        writeConts = writeConts.copyOf(n); writeBufs = writeBufs.copyOf(n); writeTotals = writeTotals.copyOf(n)
        isDeferred = isDeferred.copyOf(n)
        readJobs = readJobs.copyOf(n); writeJobs = writeJobs.copyOf(n)
        servedRound = servedRound.copyOf(n)
        readCancelHandles = readCancelHandles.copyOf(n); writeCancelHandles = writeCancelHandles.copyOf(n)
        persistent = persistent.copyOf(n); readyRead = readyRead.copyOf(n)
        pinnedArrays = pinnedArrays.copyOf(n); pins = pins.copyOf(n)
        wPinnedArrays = wPinnedArrays.copyOf(n); wPins = wPins.copyOf(n); parkEpoch = parkEpoch.copyOf(n)
        peerClosed = peerClosed.copyOf(n); sendsFail = sendsFail.copyOf(n)
        specPending = specPending.copyOf(n)
    }

    private fun pinFor(fd: Int, array: ByteArray): Pinned<ByteArray> {
        if (pinnedArrays[fd] === array) return pins[fd]!!
        pins[fd]?.unpin()
        val p = array.pin()
        pinnedArrays[fd] = array; pins[fd] = p
        return p
    }

    private fun pinForWrite(fd: Int, array: ByteArray): Pinned<ByteArray> {
        if (wPinnedArrays[fd] === array) return wPins[fd]!!
        wPins[fd]?.unpin()
        val p = array.pin()
        wPinnedArrays[fd] = array; wPins[fd] = p
        return p
    }

    /**
     * Give back the pooled arrays of reads that stayed parked: all of them when the reactor went
     * idle ([all]), otherwise those parked since before the previous sweep (SPEC §24).
     */
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

    override fun registerStream(fd: Int) {
        ensureFd(fd)
        if (poller.persistentRead) { persistent[fd] = true; poller.watchRead(fd) }
    }

    private suspend fun waitReadable(fd: Int): Unit = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = false, cont)
        readWaiters[fd] = cont
        if (!persistent[fd]) poller.armRead(fd)
        COROUTINE_SUSPENDED
    }

    private suspend fun waitWritable(fd: Int): Unit = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = true, cont)
        writeWaiters[fd] = cont
        poller.armWrite(fd)
        COROUTINE_SUSPENDED
    }

    /**
     * Before parking: a coroutine that is already cancelled must not park (nothing would wake it),
     * and its Job's cancellation must be able to wake it. The handler is registered on the first
     * park of this coroutine on this side of [fd] and reused afterwards.
     */
    private fun watchCancellation(fd: Int, write: Boolean, cont: Continuation<*>) {
        val job = cont.context[Job] ?: return
        if (!job.isActive) throw job.getCancellationException()
        val jobs = if (write) writeJobs else readJobs
        if (jobs[fd] === job) return
        val handles = if (write) writeCancelHandles else readCancelHandles
        handles[fd]?.dispose()
        jobs[fd] = job
        // Cancellation may come from any thread; the waiter slots are reactor-thread state.
        handles[fd] = job.invokeOnCompletion(onCancelling = true, invokeImmediately = false) { cause ->
            // Also called when the job completes normally (cause == null): nothing is parked then, and
            // building a cancellation exception (a stack walk) per completed job cost a request's worth
            // of CPU on short-lived jobs such as withContext / withTimeout (SPEC §24.12).
            if (cause != null) postToReactor { onParkCancelled(fd, write, job) }
        }
        // SPEC §27.9: a cancel landing between the isActive check above and this registration does not
        // call the handler (invokeImmediately = false); see it here, before parking. One after the
        // registration comes from another thread, so its wake-up is queued behind this park.
        if (!job.isActive) { handles[fd]?.dispose(); handles[fd] = null; jobs[fd] = null; throw job.getCancellationException() }
    }

    private fun onParkCancelled(fd: Int, write: Boolean, job: Job) {
        if (fd >= readWaiters.size) return
        val waiters = if (write) writeWaiters else readWaiters
        val w = waiters[fd]
        if (w != null && w.context[Job] === job) { waiters[fd] = null; enqueueResume(w, job.getCancellationException()) }
        // The slot may now belong to another coroutine: only this job's parked op is woken.
        if (write) { if (writeConts[fd]?.context?.get(Job) === job) finishWrite(fd, 0, job.getCancellationException()) }
        else if (readConts[fd]?.context?.get(Job) === job) finishRead(fd, 0, job.getCancellationException())
    }

    /** SPEC §23.2: wake the parked reader/writer on [fd] (also a reader deferred by §19.5) with [cause]. */
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

    // ---- reads (SPEC §24). `read` is a non-suspending attempt plus, when it must wait, a tail call
    // into [parkRead]; the recv after a wait is done by [completeRead] on the reactor's own turn.

    override suspend fun read(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        ensureFd(fd)
        // SPEC §19.5: one successful recv per connection per poll round; SPEC §17: a persistent fd
        // whose edge was consumed waits for the next edge instead of a speculative recv.
        if (servedRound[fd] != round && (!persistent[fd] || readyRead[fd])) {
            val n = tryRecv(fd, dst, sizer)
            if (n != RECV_WAIT) return intResult(n)
        }
        return parkRead(fd, dst, sizer)
    }

    /** One recv into [dst]: bytes (> 0), -1 at EOF, or [RECV_WAIT]; throws on a socket error. */
    private fun tryRecv(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        val cap = dst.reserve(sizer.readChunk(), bufferPool)     // may replace the backing array: pin after
        val n = recvPinned(fd, pinFor(fd, dst.backingArray()), dst.writerIndex(), cap)
        if (specPending[fd]) {
            specPending[fd] = false
            specSeen++
            if (n == WOULD_BLOCK) specMissed++
            // Skip only when speculation almost never pays (> 90 % misses): at a ~50 % miss rate (4 cores,
            // fast clients) skipping cost more edge round trips than it saved recvs (v40: 0.91 vs 0.96).
            if (specSeen >= 256) { skipSpeculation = specMissed * 10 > specSeen * 9; specSeen = 0; specMissed = 0 }
        }
        stats?.let { it.reads++; if (n > 0) it.readBytes += n else if (n == WOULD_BLOCK) it.readsWouldBlock++ }
        return when {
            // Deliberately no short-read rule (SPEC §17b, rejected): the ready flag stays set after a
            // successful recv, so the next read() picks up data that arrived meanwhile without a poll.
            n > 0 -> {
                dst.commitWrite(n); servedRound[fd] = round; sizer.onRead(n)
                if (n < cap && persistent[fd] && shortReadMode != SHORT_OFF) afterShortRead(fd)
                n
            }
            n == EOF_RESULT -> -1
            n == WOULD_BLOCK -> { readyRead[fd] = false; RECV_WAIT }
            else -> { val e = lastSocketError(); throw IoException("read failed: ${errnoMessage(e)}", e) }
        }
    }

    /** A short recv on an edge-triggered fd: speculate on the next recv, or wait for the next edge (SPEC §24.5). */
    private fun afterShortRead(fd: Int) {
        if (peerClosed[fd]) return                          // a FIN may already be in: keep reading until EOF
        val skip = shortReadMode == SHORT_ALWAYS || (skipSpeculation && (++shortCount and 15) != 0)
        if (skip) readyRead[fd] = false else specPending[fd] = true
    }

    private suspend fun parkRead(fd: Int, dst: Buffer, sizer: ReadSizer): Int = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = false, cont)
        // Parking keeps the array (a quick wake reuses it and its pin); the idle sweep takes it back.
        if (dst.holdsIdleArray) { parkEpoch[fd] = sweepEpoch; idleArrays = true }
        readConts[fd] = cont; readBufs[fd] = dst; readSizers[fd] = sizer
        if (servedRound[fd] == round && (!persistent[fd] || readyRead[fd])) defer(fd)
        else if (!persistent[fd]) poller.armRead(fd)
        COROUTINE_SUSPENDED
    }

    /** Retry [fd]'s parked read in the next round (SPEC §19.5). */
    private fun defer(fd: Int) {
        if (isDeferred[fd]) return
        if (deferredCount == deferredFds.size) deferredFds = deferredFds.copyOf(deferredCount * 2)
        deferredFds[deferredCount++] = fd
        isDeferred[fd] = true
    }

    /** Readiness (or a deferred turn) for a parked read: do the recv, resume the reader if it completed. */
    private fun completeRead(fd: Int) {
        if (servedRound[fd] == round) { defer(fd); return }
        val n = try { tryRecv(fd, readBufs[fd]!!, readSizers[fd]!!) } catch (t: Throwable) { finishRead(fd, 0, t); return }
        if (n == RECV_WAIT) { if (!persistent[fd]) poller.armRead(fd); return }
        finishRead(fd, n, null)
    }

    private fun finishRead(fd: Int, n: Int, error: Throwable?) {
        val c = readConts[fd] ?: return
        readConts[fd] = null; readBufs[fd] = null; readSizers[fd] = null
        enqueueResumeInt(c, n, error)
    }

    // ---- writes (SPEC §24): send until done or would-block; the rest is sent by [completeWrite].

    override suspend fun write(fd: Int, src: Buffer): Int {
        ensureFd(fd)
        if (sendsFail[fd]) throw IoException("send failed: peer closed (EPIPE)", platform.posix.EPIPE)
        var total = 0
        while (src.readableBytes > 0) {
            val n = sendPinned(fd, pinForWrite(fd, src.backingArray()), src.readerIndex(), src.readableBytes)
            stats?.let { it.writes++; if (n >= 0) it.writeBytes += n else if (n == WOULD_BLOCK) it.writesWouldBlock++ }
            if (n >= 0) { src.consumeSent(n); total += n }
            else if (n == WOULD_BLOCK) return parkWrite(fd, src, total)
            else { val e = lastSocketError(); throw IoException("write failed: ${errnoMessage(e)}", e) }
        }
        return intResult(total)
    }

    private suspend fun parkWrite(fd: Int, src: Buffer, sentSoFar: Int): Int = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, write = true, cont)
        writeConts[fd] = cont; writeBufs[fd] = src; writeTotals[fd] = sentSoFar
        poller.armWrite(fd)
        COROUTINE_SUSPENDED
    }

    private fun completeWrite(fd: Int) {
        val src = writeBufs[fd]!!
        var total = writeTotals[fd]
        while (src.readableBytes > 0) {
            val n = sendPinned(fd, pinForWrite(fd, src.backingArray()), src.readerIndex(), src.readableBytes)
            stats?.let { it.writes++; if (n >= 0) it.writeBytes += n else if (n == WOULD_BLOCK) it.writesWouldBlock++ }
            if (n >= 0) { src.consumeSent(n); total += n }
            else if (n == WOULD_BLOCK) { writeTotals[fd] = total; poller.armWrite(fd); return }
            else { val e = lastSocketError(); finishWrite(fd, 0, IoException("write failed: ${errnoMessage(e)}", e)); return }
        }
        finishWrite(fd, total, null)
    }

    private fun finishWrite(fd: Int, total: Int, error: Throwable?) {
        val c = writeConts[fd] ?: return
        writeConts[fd] = null; writeBufs[fd] = null
        enqueueResumeInt(c, total, error)
    }

    /** One sendmsg per batch of up to [MAX_IOV] buffers; parks on would-block like [write] (SPEC §23.3). */
    override suspend fun writev(fd: Int, bufs: Array<Buffer>, count: Int): Long {
        ensureFd(fd)
        if (sendsFail[fd]) throw IoException("send failed: peer closed (EPIPE)", platform.posix.EPIPE)
        var total = 0L
        var i = 0
        while (i < count && bufs[i].readableBytes == 0) i++
        while (i < count) {
            val batch = minOf(MAX_IOV, count - i)
            val n = sendBuffers(fd, bufs, i, batch)
            stats?.let { it.writes++; if (n >= 0) it.writeBytes += n else if (n == WOULD_BLOCK.toLong()) it.writesWouldBlock++ }
            when {
                n >= 0 -> { total += n; i = advanceBuffers(bufs, i, count, n) }
                n == WOULD_BLOCK.toLong() -> waitWritable(fd)
                else -> { val e = lastSocketError(); throw IoException("writev failed: ${errnoMessage(e)}", e) }
            }
        }
        return total
    }

    override suspend fun accept(listenFd: Int): Int {
        ensureFd(listenFd)
        while (true) {
            val clientFd = acceptOne(listenFd)
            if (clientFd >= 0) {
                setNonBlocking(clientFd)
                // Apple rejects SO_NOSIGPIPE once the peer has closed or reset before accept() returned;
                // a send would then kill the process. The peer is gone, so reads still deliver what it
                // sent (then EOF) and every write fails with EPIPE without calling send.
                if (!suppressSigpipe(clientFd)) { ensureFd(clientFd); sendsFail[clientFd] = true }
                return clientFd
            }
            waitReadable(listenFd)
        }
    }

    override suspend fun awaitConnect(fd: Int) { ensureFd(fd); waitWritable(fd) }

    override fun runUntil(root: Job) {
        val wakeFd = wakeReadFd                     // read once: the property is a lazy (SPEC §23.1)
        ensureFd(wakeFd)
        poller.armRead(wakeFd)
        while (true) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (readyToStop(root)) break

            var timeout = if (hasTasks() || deferredCount > 0) 0 else nextTimerMillis()
            // Parked reads hold pooled arrays: wake up after IDLE_SWEEP_MS of quiet to give them back.
            val sweepWait = idleArrays && (timeout < 0 || timeout > IDLE_SWEEP_MS)
            if (sweepWait) timeout = IDLE_SWEEP_MS
            var woke = false
            val n = poller.poll(timeout)
            for (i in 0 until n) {
                val fd = poller.readyFd(i)
                if (fd == wakeFd) { woke = true; continue }
                if (fd >= persistent.size) continue       // never registered here (cannot happen; be safe)
                val f = poller.readyFlags(i)
                if (f and READY_HUP != 0) peerClosed[fd] = true
                if (f and READY_READ != 0) {
                    // Persistent fds: record the edge first — it is not repeated. After an edge the
                    // next recv is not speculative (data arrived).
                    if (persistent[fd]) { readyRead[fd] = true; specPending[fd] = false }
                    val w = readWaiters[fd]
                    if (w != null) { readWaiters[fd] = null; enqueueResume(w) }
                    if (readConts[fd] != null) completeRead(fd)
                }
                if (f and READY_WRITE != 0) {
                    val w = writeWaiters[fd]
                    if (w != null) { writeWaiters[fd] = null; enqueueResume(w) }
                    if (writeConts[fd] != null) completeWrite(fd)
                }
            }
            if (woke) { onWake(); poller.armRead(wakeFd) }
            countPoll(timeout == 0, n)
            if (sweepWait && n == 0) sweepParked(all = true)
            else if (++roundsSinceSweep >= SWEEP_ROUNDS && idleArrays) sweepParked(all = false)
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
        // A read deferred to the next round must not recv on this fd number, which the kernel may
        // hand to a new connection: its slot is emptied here (the deferred entry then finds nothing).
        // Only a parked op needs the exception: building one walks the stack (SPEC §26.6), and
        // every connection passes through here once.
        if (readConts[fd] != null) finishRead(fd, 0, ClosedException())
        if (writeConts[fd] != null) finishWrite(fd, 0, ClosedException())
        forgetCancellation(fd)
        servedRound[fd] = 0
        persistent[fd] = false; readyRead[fd] = false; peerClosed[fd] = false; sendsFail[fd] = false
        specPending[fd] = false
        pins[fd]?.unpin(); pins[fd] = null; pinnedArrays[fd] = null
        wPins[fd]?.unpin(); wPins[fd] = null; wPinnedArrays[fd] = null
        poller.forget(fd)
        closeFd(fd)
    }

    private companion object {
        /** [tryRecv]: nothing to read yet. Distinct from -1, which is EOF here (WOULD_BLOCK is also -1). */
        const val RECV_WAIT = Int.MIN_VALUE
        const val SHORT_OFF = 0
        const val SHORT_ALWAYS = 1
        const val SHORT_AUTO = 2
        /** Quiet time after which parked reads give their pooled arrays back. */
        const val IDLE_SWEEP_MS = 50
        /** Under load, reads parked for a whole interval of this many rounds give theirs back. */
        const val SWEEP_ROUNDS = 1024
    }

    override fun shutdown() {
        poller.close()
        closeWakePipe()
    }
}
