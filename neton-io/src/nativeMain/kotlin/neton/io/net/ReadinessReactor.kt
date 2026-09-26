package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.pin
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
    private var readWaiters = arrayOfNulls<Continuation<Unit>>(64)
    private var writeWaiters = arrayOfNulls<Continuation<Unit>>(64)
    // Cancellation is watched once per (fd, direction, coroutine Job), not once per park: the
    // handle is reused for every park by the same coroutine and disposed on close or when another
    // coroutine takes over that side of the stream.
    // SPEC §19.5 fairness: the poll round in which each fd last had a successful recv, and the
    // readers deferred to the next round because they already had theirs in this one.
    private var servedRound = IntArray(64)
    private var round = 1
    private var deferredConts = arrayOfNulls<Continuation<Unit>>(64)
    private var deferredFds = IntArray(64)
    private var deferredCount = 0
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
    private var pinnedArrays = arrayOfNulls<ByteArray>(64)
    private var pins = arrayOfNulls<Pinned<ByteArray>>(64)

    private fun ensureFd(fd: Int) {
        if (fd < persistent.size) return
        var n = persistent.size
        while (n <= fd) n *= 2
        readWaiters = readWaiters.copyOf(n); writeWaiters = writeWaiters.copyOf(n)
        readJobs = readJobs.copyOf(n); writeJobs = writeJobs.copyOf(n)
        servedRound = servedRound.copyOf(n)
        readCancelHandles = readCancelHandles.copyOf(n); writeCancelHandles = writeCancelHandles.copyOf(n)
        persistent = persistent.copyOf(n); readyRead = readyRead.copyOf(n)
        pinnedArrays = pinnedArrays.copyOf(n); pins = pins.copyOf(n)
    }

    private fun pinFor(fd: Int, array: ByteArray): Pinned<ByteArray> {
        if (pinnedArrays[fd] === array) return pins[fd]!!
        pins[fd]?.unpin()
        val p = array.pin()
        pinnedArrays[fd] = array; pins[fd] = p
        return p
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
    private fun watchCancellation(fd: Int, write: Boolean, cont: Continuation<Unit>) {
        val job = cont.context[Job] ?: return
        if (!job.isActive) throw job.getCancellationException()
        val jobs = if (write) writeJobs else readJobs
        if (jobs[fd] === job) return
        val handles = if (write) writeCancelHandles else readCancelHandles
        handles[fd]?.dispose()
        jobs[fd] = job
        // Cancellation may come from any thread; the waiter slots are reactor-thread state.
        handles[fd] = job.invokeOnCompletion(onCancelling = true, invokeImmediately = false) {
            postToReactor { onParkCancelled(fd, write, job) }
        }
    }

    private fun onParkCancelled(fd: Int, write: Boolean, job: Job) {
        if (fd >= readWaiters.size) return
        val waiters = if (write) writeWaiters else readWaiters
        val w = waiters[fd] ?: return
        if (w.context[Job] !== job) return              // the slot now belongs to another coroutine
        waiters[fd] = null
        enqueueResume(w, job.getCancellationException())
    }

    /** SPEC §23.2: wake the parked reader/writer on [fd] (and a reader deferred by §19.5) with [cause]. */
    override fun timeoutParked(fd: Int, reads: Boolean, writes: Boolean, cause: Throwable) {
        if (fd >= readWaiters.size) return
        if (reads) {
            readWaiters[fd]?.let { readWaiters[fd] = null; enqueueResume(it, cause) }
            var i = 0
            while (i < deferredCount) {
                if (deferredFds[i] == fd) {
                    val c = deferredConts[i]!!
                    deferredCount--
                    deferredConts[i] = deferredConts[deferredCount]; deferredFds[i] = deferredFds[deferredCount]
                    deferredConts[deferredCount] = null
                    enqueueResume(c, cause)
                } else i++
            }
        }
        if (writes) writeWaiters[fd]?.let { writeWaiters[fd] = null; enqueueResume(it, cause) }
    }

    private fun forgetCancellation(fd: Int) {
        readCancelHandles[fd]?.dispose(); writeCancelHandles[fd]?.dispose()
        readCancelHandles[fd] = null; writeCancelHandles[fd] = null
        readJobs[fd] = null; writeJobs[fd] = null
    }

    /**
     * SPEC §19.5: park until the next poll round. Used by a reader whose connection already had a
     * successful recv in this round, so every ready connection is served once before any is served
     * twice. Without it, a client thread woken by our send() onto this core refilled its socket at
     * once and drain-to-EAGAIN served the same connection again and again (Jain 0.29-0.40).
     */
    private suspend fun deferToNextRound(fd: Int): Unit = suspendCoroutineUninterceptedOrReturn { cont ->
        if (deferredCount == deferredConts.size) {
            deferredConts = deferredConts.copyOf(deferredCount * 2); deferredFds = deferredFds.copyOf(deferredCount * 2)
        }
        deferredConts[deferredCount] = cont; deferredFds[deferredCount] = fd; deferredCount++
        COROUTINE_SUSPENDED
    }

    override suspend fun read(fd: Int, dst: Buffer, chunk: Int): Int {
        ensureFd(fd)
        while (true) {
            // Parking: a pooled, empty buffer gives its array back first (idle connections hold none, SPEC §23.7).
            if (persistent[fd] && !readyRead[fd]) { dst.releaseIfIdle(bufferPool); waitReadable(fd); continue }
            if (servedRound[fd] == round) {
                deferToNextRound(fd)
                // A deferred reader is not in a waiter slot, so cancellation is checked here.
                kotlin.coroutines.coroutineContext[Job]?.let { if (!it.isActive) throw it.getCancellationException() }
                continue
            }
            val cap = dst.reserve(chunk, bufferPool)     // may replace the backing array: pin after
            val n = recvPinned(fd, pinFor(fd, dst.backingArray()), dst.writerIndex(), cap)
            stats?.let { it.reads++; if (n > 0) it.readBytes += n else if (n == WOULD_BLOCK) it.readsWouldBlock++ }
            when {
                // Deliberately no short-read rule here (SPEC §17b, rejected): keeping the ready
                // flag set after a successful recv lets the next read() pick up a request that
                // arrived meanwhile without a poll round; the EAGAIN recv it costs is cheap.
                n > 0 -> { dst.commitWrite(n); servedRound[fd] = round; return n }
                n == EOF_RESULT -> return -1
                n == WOULD_BLOCK -> { readyRead[fd] = false; dst.releaseIfIdle(bufferPool); waitReadable(fd) }
                else -> { val e = lastSocketError(); throw IoException("read failed: ${errnoMessage(e)}", e) }
            }
        }
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        ensureFd(fd)
        var total = 0
        while (src.readableBytes > 0) {
            val n = sendPinned(fd, pinFor(fd, src.backingArray()), src.readerIndex(), src.readableBytes)
            stats?.let { it.writes++; if (n >= 0) it.writeBytes += n else if (n == WOULD_BLOCK) it.writesWouldBlock++ }
            when {
                n >= 0 -> { src.consume(n, bufferPool); total += n }
                n == WOULD_BLOCK -> waitWritable(fd)
                else -> { val e = lastSocketError(); throw IoException("write failed: ${errnoMessage(e)}", e) }
            }
        }
        return total
    }

    /** One sendmsg per batch of up to [MAX_IOV] buffers; parks on would-block like [write] (SPEC §23.3). */
    override suspend fun writev(fd: Int, bufs: Array<Buffer>, count: Int): Long {
        ensureFd(fd)
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
                suppressSigpipe(clientFd)
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
        while (!root.isCompleted) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (root.isCompleted) break

            val timeout = if (hasTasks() || deferredCount > 0) 0 else nextTimerMillis()
            var woke = false
            val n = poller.poll(timeout) { fd, readable, writable ->
                if (fd == wakeFd) { woke = true; return@poll }
                if (fd >= persistent.size) return@poll   // never registered here (cannot happen; be safe)
                if (readable) {
                    // Persistent fds: record the edge *before* resuming, so the resumed read() sees
                    // the fd as ready instead of parking again (the edge is not repeated).
                    if (persistent[fd]) readyRead[fd] = true
                    val w = readWaiters[fd]
                    if (w != null) { readWaiters[fd] = null; enqueueResume(w) }
                }
                if (writable) {
                    val w = writeWaiters[fd]
                    if (w != null) { writeWaiters[fd] = null; enqueueResume(w) }
                }
            }
            if (woke) { onWake(); poller.armRead(wakeFd) }
            countPoll(timeout == 0, n)
            // New round: readers deferred in the last one get their turn now (SPEC §19.5).
            round++
            for (i in 0 until deferredCount) { enqueueResume(deferredConts[i]!!); deferredConts[i] = null }
            deferredCount = 0
        }
    }

    override fun closeStream(fd: Int) {
        checkOwner("close")
        ensureFd(fd)
        readWaiters[fd]?.let { readWaiters[fd] = null; enqueueResume(it, ClosedException()) }
        writeWaiters[fd]?.let { writeWaiters[fd] = null; enqueueResume(it, ClosedException()) }
        forgetCancellation(fd)
        // A reader deferred to the next round must not run recv on this fd number, which the
        // kernel may hand to a new connection: fail it now like a parked reader.
        var i = 0
        while (i < deferredCount) {
            if (deferredFds[i] == fd) {
                val c = deferredConts[i]!!
                deferredCount--
                deferredConts[i] = deferredConts[deferredCount]; deferredFds[i] = deferredFds[deferredCount]
                deferredConts[deferredCount] = null
                enqueueResume(c, ClosedException())
            } else i++
        }
        servedRound[fd] = 0
        persistent[fd] = false; readyRead[fd] = false
        pins[fd]?.unpin(); pins[fd] = null; pinnedArrays[fd] = null
        poller.forget(fd)
        closeFd(fd)
    }

    override fun shutdown() {
        poller.close()
        closeWakePipe()
    }
}
