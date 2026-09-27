package neton.io.net

import platform.posix.msghdr

import platform.posix.iovec

import kotlinx.cinterop.get

import neton.io.uring.NETON_IORING_OP_SENDMSG

import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.pin
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.toKString
import kotlinx.cinterop.toLong
import kotlinx.cinterop.value
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlinx.coroutines.Job
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.plus
import kotlinx.cinterop.reinterpret
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import kotlin.coroutines.resumeWithException
import neton.io.uring.NETON_IORING_ENTER_GETEVENTS
import neton.io.uring.NETON_IORING_SETUP_COOP_TASKRUN
import neton.io.uring.NETON_IORING_SETUP_DEFER_TASKRUN
import neton.io.uring.NETON_IORING_SETUP_SINGLE_ISSUER
import neton.io.uring.NETON_IORING_OFF_CQ_RING
import neton.io.uring.NETON_IORING_OFF_SQES
import neton.io.uring.NETON_IORING_OFF_SQ_RING
import neton.io.uring.NETON_IORING_OP_ACCEPT
import neton.io.uring.NETON_IORING_OP_ASYNC_CANCEL
import neton.io.uring.NETON_IORING_OP_POLL_ADD
import neton.io.uring.NETON_IORING_OP_READ
import neton.io.uring.NETON_IORING_OP_SEND
import neton.io.uring.NETON_IORING_OP_TIMEOUT
import neton.io.uring.NETON_IORING_OP_RECV
import neton.io.uring.NETON_IORING_OP_PROVIDE_BUFFERS
import neton.io.uring.NETON_IOSQE_BUFFER_SELECT
import neton.io.uring.NETON_IORING_RECV_MULTISHOT
import neton.io.uring.NETON_IORING_CQE_F_BUFFER
import neton.io.uring.NETON_IORING_CQE_F_MORE
import neton.io.uring.NETON_IORING_CQE_BUFFER_SHIFT
import neton.io.uring.neton_kernel_timespec
import neton.io.uring.NETON_MSG_NOSIGNAL
import neton.io.uring.NETON_IOSQE_FIXED_FILE
import neton.io.uring.neton_uring_register_sparse_files
import neton.io.uring.neton_uring_update_file
import neton.io.uring.NETON_POLLIN
import neton.io.uring.NETON_POLLOUT
import neton.io.uring.neton_array_at
import neton.io.uring.neton_cqe_at
import neton.io.uring.neton_io_uring_params
import neton.io.uring.neton_load32
import neton.io.uring.neton_sqe_at
import neton.io.uring.neton_store32
import neton.io.uring.neton_uring_enter
import neton.io.uring.neton_uring_setup
import platform.posix.MAP_POPULATE
import platform.posix.MAP_SHARED
import platform.posix.PROT_READ
import platform.posix.PROT_WRITE
import platform.posix.close
import platform.posix.fprintf
import platform.posix.stderr
import platform.posix.getenv
import platform.posix.memset
import platform.posix.memcpy
import platform.posix.malloc
import platform.posix.free
import platform.posix.ENOBUFS
import platform.posix.ECANCELED
import platform.posix.mmap
import platform.posix.munmap
import kotlin.coroutines.resume

/**
 * Completion reactor backed by io_uring: a read/write/accept submits the op (with its buffer) and
 * suspends until the completion carries the result. Single-threaded — submit and reap happen on one
 * thread around io_uring_enter (a full barrier), so no SMP ring barriers are needed.
 *
 * `to_submit` is always the number of unconsumed SQEs (`sq_tail - sq_head`) read straight from the
 * ring, so a partial submit is handled naturally by the next enter. Before writing a new SQE the
 * ring is drained if full, so submitting more ops than the ring holds never overwrites an
 * unconsumed entry (correctness does not depend on the ring being large enough).
 *
 * Buffer lifetime (completion model): the kernel may write into an op's buffer until that op's CQE
 * has been reaped, so a buffer stays pinned from submit until its CQE, unconditionally. Cancelling
 * the coroutine that awaits an op does not release anything: it submits IORING_OP_ASYNC_CANCEL for
 * the op and drops the continuation; the op's own CQE (real result or -ECANCELED) arrives exactly
 * once either way and is what unpins the buffer. A cancel that loses the race with normal
 * completion reports -ENOENT/-EALREADY on its own CQE, which is ignored. On shutdown every op still
 * in flight is cancelled and the ring is drained until their CQEs arrive before the ring is closed;
 * if some never arrive the buffers are intentionally leaked (kept pinned), never freed under the
 * kernel. Cancellation must originate on the reactor thread (the reactor is single-threaded).
 */
@OptIn(ExperimentalForeignApi::class, InternalCoroutinesApi::class)
internal class UringReactor : Reactor() {

    private val ringFd: Int
    private val sqBase: COpaquePointer
    private val cqBase: COpaquePointer
    private val sqes: COpaquePointer

    // Mapping lengths, kept so shutdown() can munmap exactly what init mapped (P1-7).
    private val sqMapLen: ULong
    private val cqMapLen: ULong
    private val sqesMapLen: ULong

    private val sqHeadOff: UInt
    private val sqTailOff: UInt
    private val sqMaskOff: UInt
    private val sqArrayOff: UInt
    private val sqEntries: UInt
    private val cqHeadOff: UInt
    private val cqTailOff: UInt
    private val cqMaskOff: UInt
    private val cqesOff: UInt

    /** True when the ring was created with COOP_TASKRUN|SINGLE_ISSUER|DEFER_TASKRUN. */
    private var modernSetup = false
    override val driverName: String get() = (if (modernSetup) "iouring+defer" else "iouring") + (if (multishot) "+multishot" else "")

    // ---- in-flight ops: a slot table instead of a HashMap<ULong, …> (SPEC §17c: no boxed keys, no
    // per-op objects). user_data = slot index << 32 | generation; the generation makes a stale CQE
    // for a reused slot harmless. Control ops (wake poll, timeout, cancel) use CONTROL_BASE + n.
    // SPEC §19.3 (io_uring half): the awaiting coroutine is stored as its raw continuation; every path
    // that resumes it (CQE, close, cancellation) takes it out of the slot first.
    private class Slot { var live = false; var gen = 0u; var fd = -1; var pin: PinRef? = null; var cont: Continuation<Int>? = null; var multishot = false; var cancelOnAbort = true; var isWrite = false
        // SPEC §23.3: a vectored send owns extra pins and a native msghdr + iovec block until its CQE.
        var extraPins: Array<Pinned<ByteArray>>? = null; var nativeBlock: CPointer<ByteVar>? = null
        // SPEC §24: a whole-buffer send completed by the reactor (resubmitted after short sends).
        var sendBuf: Buffer? = null; var sendTotal = 0
        // A whole-buffer send whose writer was cancelled or timed out: the SEND is cancelled in the
        // kernel and the writer is resumed with this once its CQE has said how much went out.
        var sendAbort: Throwable? = null
        // SPEC §24.7: a single RECV straight into the reader's buffer, completed by the reactor.
        var readBuf: Buffer? = null; var readSizer: ReadSizer? = null; var readAbort: Throwable? = null
        // SPEC §26.5: where a single read is (RS_*), and the sweep epoch its RECV was submitted in.
        var readState = RS_RECV; var parkEpoch = 0 }
    private var slots = arrayOfNulls<Slot>(256)
    private var freeSlots = IntArray(256) { it }
    private var freeTop = 256           // freeSlots[0 until freeTop] are free
    private var liveOps = 0
    private var nextControl = 0uL

    private fun controlUd(): ULong = CONTROL_BASE or (nextControl++ and 0xFFFF_FFFFuL)

    /** Reserve a slot; returns its index (the [Slot] itself is `slots[index]`). */
    private fun takeSlot(): Int {
        if (freeTop == 0) {
            val n = slots.size
            slots = slots.copyOf(n * 2)
            freeSlots = IntArray(n * 2)
            for (i in 0 until n) freeSlots[i] = n + i
            freeTop = n
        }
        val idx = freeSlots[--freeTop]
        val slot = slots[idx] ?: Slot().also { slots[idx] = it }
        slot.live = true; slot.gen++
        liveOps++
        return idx
    }

    private fun releaseSlot(idx: Int, slot: Slot) {
        slot.live = false; slot.cont = null; slot.pin = null; slot.fd = -1; slot.multishot = false; slot.isWrite = false
        slot.sendBuf = null; slot.sendAbort = null
        slot.readBuf = null; slot.readSizer = null; slot.readAbort = null
        if (slot.nativeBlock != null) releaseVectored(slot)     // writev only; keeps this path small enough to inline
        freeSlots[freeTop++] = idx
        liveOps--
    }

    /** Reached only after the op's CQE (or before its SQE existed): the kernel no longer touches these. */
    private fun releaseVectored(slot: Slot) {
        slot.extraPins?.let { for (p in it) p.unpin() }; slot.extraPins = null
        slot.nativeBlock?.let { free(it) }; slot.nativeBlock = null
    }

    // ---- SPEC §17c step 6: multishot recv with provided buffers. One IORING_OP_RECV|MULTISHOT per
    // connection stays armed and completes once per arriving chunk into a kernel-selected buffer
    // from a pool this reactor provided (IORING_OP_PROVIDE_BUFFERS). read() copies the chunk into
    // the caller's Buffer and gives the pool buffer back. Per request this replaces "submit recv →
    // EAGAIN → arm poll → wake → recv" with "wake → recv → CQE" inside the kernel, and no recv SQE.
    // NETON_IO_URING_MULTISHOT=0 disables it (A/B); NETON_IO_URING_BUFS / _BUFSZ size the pool.
    private val multishot: Boolean
    private val bufCount: Int
    private val bufSize: Int
    private var bufBase: CPointer<ByteVar>? = null
    private var freeBufs = 0
    // per fd: queue of completed chunks (bid shl 32 | len), the parked reader, terminal state
    private var rq = arrayOfNulls<LongArray>(64)
    private var rqHead = IntArray(64); private var rqCount = IntArray(64)
    private var msArmed = BooleanArray(64)
    // SPEC §23.2 read-side backpressure: bytes received but not yet read per fd, whether the multishot
    // op was paused for it, and the armed op's user_data (to cancel it).
    private var rqBytes = IntArray(64)
    private var msPaused = BooleanArray(64)
    private var msUd = LongArray(64)
    // SPEC §24.7: single RECVs carry IORING_RECVSEND_POLL_FIRST. Without it a RECV submitted while data
    // is waiting completes inline at submission, and busy connections kept that lane while others'
    // armed RECVs did not complete for seconds (153, 1000 conns: ~10 % of connections served once in
    // 8 s). With it every read waits for readiness first and completes in arrival order (as geario
    // does). NETON_IO_URING_POLL_FIRST=0 turns it off.
    private val recvIoprio: Int = if (getenv("NETON_IO_URING_POLL_FIRST")?.toKString() == "0") 0 else IORING_RECVSEND_POLL_FIRST
    private val maxQueuedPerConn: Int = getenv("NETON_IO_URING_MAX_QUEUED")?.toKString()?.toIntOrNull() ?: (256 * 1024)
    private var msEof = BooleanArray(64)
    private var msErr = IntArray(64)
    // SPEC §24: the parked reader's own continuation plus its buffer; chunks are copied in by the
    // reactor after reaping, and the reader is resumed with the byte count.
    private var readers = arrayOfNulls<Continuation<Int>>(64)
    private var readerBufs = arrayOfNulls<Buffer>(64)
    private var readerSizers = arrayOfNulls<ReadSizer>(64)
    // SPEC §24 idle sweep (as in ReadinessReactor): parked readers give their pooled arrays back
    // after IDLE_SWEEP_MS of quiet, or when parked across a whole interval of SWEEP_ROUNDS rounds.
    private var parkEpoch = IntArray(64)
    private var sweepEpoch = 1
    private var roundsSinceSweep = 0
    private var idleArrays = false
    private var timeoutFired = false
    private var msReady = IntArray(64); private var msReadyCount = 0; private var isMsReady = BooleanArray(64)
    // Send slots whose short send is resubmitted after reaping (never from inside reap()).
    private var pendingSends = IntArray(64); private var pendingSendCount = 0
    // SPEC §26.5: single reads between two ops (after a demoting cancel or a POLLIN), advanced after reaping.
    private var pendingReads = IntArray(64); private var pendingReadCount = 0
    // SPEC §26.5: an idle single RECV gives its buffer back and waits in a POLL_ADD instead.
    // NETON_IO_URING_IDLE_POLL=0 turns it off (A/B).
    private val idlePoll: Boolean = getenv("NETON_IO_URING_IDLE_POLL")?.toKString() != "0"
    private var starved = IntArray(64); private var starvedCount = 0   // fds waiting for a free pool buffer

    private fun ensureMsFd(fd: Int) {
        if (fd < msArmed.size) return
        var n = msArmed.size
        while (n <= fd) n *= 2
        rq = rq.copyOf(n); rqHead = rqHead.copyOf(n); rqCount = rqCount.copyOf(n)
        msArmed = msArmed.copyOf(n); msEof = msEof.copyOf(n); msErr = msErr.copyOf(n)
        rqBytes = rqBytes.copyOf(n); msPaused = msPaused.copyOf(n); msUd = msUd.copyOf(n)
        readers = readers.copyOf(n); starved = starved.copyOf(n)
        readerBufs = readerBufs.copyOf(n); readerSizers = readerSizers.copyOf(n); isMsReady = isMsReady.copyOf(n)
        parkEpoch = parkEpoch.copyOf(n)
    }

    private fun rqPush(fd: Int, bid: Int, len: Int) {
        var q = rq[fd]
        if (q == null) { q = LongArray(16); rq[fd] = q }
        if (rqCount[fd] == q.size) {
            val bigger = LongArray(q.size * 2)
            for (i in 0 until rqCount[fd]) bigger[i] = q[(rqHead[fd] + i) % q.size]
            rq[fd] = bigger; rqHead[fd] = 0; q = bigger
        }
        q[(rqHead[fd] + rqCount[fd]) % q.size] = (bid.toLong() shl 32) or len.toLong()
        rqCount[fd]++
    }

    private fun rqPop(fd: Int): Long {
        val q = rq[fd]!!
        val v = q[rqHead[fd]]
        rqHead[fd] = (rqHead[fd] + 1) % q.size; rqCount[fd]--
        return v
    }

    /** Give pool buffer [bid] back to the kernel (one SQE, no syscall of its own). */
    private fun reprovide(bid: Int) {
        prepSqe(NETON_IORING_OP_PROVIDE_BUFFERS, 1, (bufBase!! + bid * bufSize)!!.toLong(), bufSize, 0, controlUd(), off = bid.toULong(), bufGroup = BUF_GROUP)
        freeBufs++
        if (starvedCount > 0) { val f = starved[--starvedCount]; if (!msArmed[f] && !msEof[f]) armMultishot(f) }
    }

    private fun armMultishot(fd: Int) {
        if (freeBufs == 0) { starved[starvedCount++] = fd; return }
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.multishot = true
        msUd[fd] = ((idx.toULong() shl 32) or slot.gen.toULong()).toLong()
        prepSqe(NETON_IORING_OP_RECV, fd, 0L, 0, 0, (idx.toULong() shl 32) or slot.gen.toULong(),
            sqeFlags = NETON_IOSQE_BUFFER_SELECT.toInt(), ioprio = NETON_IORING_RECV_MULTISHOT.toInt(), bufGroup = BUF_GROUP)
        msArmed[fd] = true
    }

    /** A CQE for a multishot recv slot: queue the chunk (or record EOF/error), keep or drop the arm, wake the reader. */
    private fun onMultishotCqe(idx: Int, slot: Slot, res: Int, flags: UInt) {
        val fd = slot.fd
        if (res > 0 && (flags and NETON_IORING_CQE_F_BUFFER.toUInt()) != 0u) {
            freeBufs--
            rqPush(fd, (flags shr NETON_IORING_CQE_BUFFER_SHIFT).toInt(), res)
            rqBytes[fd] += res
            // Read-side backpressure: a connection whose reader has fallen this far behind stops
            // taking pool buffers from the others; re-armed once drained below half (readMultishot).
            if (rqBytes[fd] >= maxQueuedPerConn && msArmed[fd] && !msPaused[fd]) {
                msPaused[fd] = true
                requestCancel(msUd[fd].toULong())
            }
            stats?.let { it.reads++; it.readBytes += res }
        } else if (res == 0) msEof[fd] = true
        else if (res < 0 && -res != ENOBUFS && -res != ECANCELED) msErr[fd] = -res
        if ((flags and NETON_IORING_CQE_F_MORE.toUInt()) == 0u) { msArmed[fd] = false; releaseSlot(idx, slot) }
        if (readers[fd] != null && !isMsReady[fd]) {
            if (msReadyCount == msReady.size) msReady = msReady.copyOf(msReadyCount * 2)
            msReady[msReadyCount++] = fd; isMsReady[fd] = true
        }
    }

    // ---- multishot reads (SPEC §24): a non-suspending take, or a tail call into [msPark].
    private suspend fun readMultishot(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        ensureMsFd(fd)
        val n = msTake(fd, dst, sizer)
        if (n != NOTHING_QUEUED) return intResult(n)
        if (!msArmed[fd] && !msPaused[fd]) armMultishot(fd)
        return msPark(fd, dst, sizer)
    }

    /** The next queued chunk copied into [dst] (its length), -1 at EOF, or [NOTHING_QUEUED]; throws a recorded error. */
    private fun msTake(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        if (rqCount[fd] > 0) {
            val e = rqPop(fd)
            val bid = (e shr 32).toInt(); val len = (e and 0xFFFF_FFFFL).toInt()
            rqBytes[fd] -= len
            if (msPaused[fd] && rqBytes[fd] < maxQueuedPerConn / 2) {
                msPaused[fd] = false
                if (!msArmed[fd] && !msEof[fd]) armMultishot(fd)
            }
            dst.reserve(len, bufferPool)
            val pin = pinFor(fd, dst.backingArray())
            memcpy(pin.pinned.addressOf(dst.writerIndex()), bufBase!! + bid * bufSize, len.convert())
            dst.commitWrite(len)
            reprovide(bid)
            sizer.onRead(len)
            return len
        }
        if (msEof[fd]) return -1
        if (msErr[fd] != 0) { val e = msErr[fd]; msErr[fd] = 0; throw IoException("io_uring recv failed: ${errnoMessage(e)}", e) }
        return NOTHING_QUEUED
    }

    // The multishot op stays armed across a cancelled read; only the parked reader is woken.
    private suspend fun msPark(fd: Int, dst: Buffer, sizer: ReadSizer): Int = suspendCoroutineUninterceptedOrReturn { cont ->
        watchCancellation(fd, cont)
        // Parking keeps the array (a quick wake reuses it and its pin); the idle sweep takes it back.
        if (dst.holdsIdleArray) { parkEpoch[fd] = sweepEpoch; idleArrays = true }
        readers[fd] = cont; readerBufs[fd] = dst; readerSizers[fd] = sizer
        COROUTINE_SUSPENDED
    }

    private fun finishMsRead(fd: Int, n: Int, error: Throwable?) {
        val c = readers[fd] ?: return
        readers[fd] = null; readerBufs[fd] = null; readerSizers[fd] = null
        enqueueResumeInt(c, n, error)
    }

    private fun sweepParked(all: Boolean) {
        idleArrays = false
        for (fd in 0 until readerBufs.size) {
            val b = readerBufs[fd] ?: continue
            if (!b.holdsIdleArray) continue
            if (all || parkEpoch[fd] < sweepEpoch) b.releaseIfIdle(bufferPool) else idleArrays = true
        }
        if (idlePoll && !multishot) demoteIdleReads(all)
        sweepEpoch++
        roundsSinceSweep = 0
    }

    /** After reaping: parked readers whose fd got data (or EOF / an error) take it now. */
    private fun completeReadyReads() {
        val count = msReadyCount
        msReadyCount = 0
        for (i in 0 until count) {
            val fd = msReady[i]
            isMsReady[fd] = false
            val dst = readerBufs[fd] ?: continue
            val n = try { msTake(fd, dst, readerSizers[fd]!!) } catch (t: Throwable) { finishMsRead(fd, 0, t); continue }
            if (n == NOTHING_QUEUED) { if (!msArmed[fd] && !msPaused[fd] && !msEof[fd]) armMultishot(fd); continue }
            finishMsRead(fd, n, null)
        }
    }

    // ---- pins: one per fd for the buffer last used on it, ref-counted by the ops that own it. The
    // kernel may touch a buffer until the op's CQE, so a pin is released only when no op holds it
    // *and* it has been retired (buffer replaced, or fd closed).
    private class PinRef(val array: ByteArray, val pinned: Pinned<ByteArray>) { var refs = 0; var retired = false
        fun release() { if (retired && refs == 0) pinned.unpin() } }
    private var pinRefs = arrayOfNulls<PinRef>(64)
    private var wPinRefs = arrayOfNulls<PinRef>(64)       // write side (SPEC §24: no re-pin per request)

    // Cancellation is watched once per (fd, coroutine Job): two watches per fd, because a stream's
    // read loop and write loop are usually different coroutines. On cancellation (any thread) the
    // handler hops to the reactor and wakes that job's parked continuations on that fd.
    private var watchJobA = arrayOfNulls<Job>(64); private var watchHandleA = arrayOfNulls<DisposableHandle>(64)
    private var watchJobB = arrayOfNulls<Job>(64); private var watchHandleB = arrayOfNulls<DisposableHandle>(64)

    private fun ensureWatch(fd: Int) {
        if (fd < watchJobA.size) return
        var n = watchJobA.size
        while (n <= fd) n *= 2
        watchJobA = watchJobA.copyOf(n); watchHandleA = watchHandleA.copyOf(n)
        watchJobB = watchJobB.copyOf(n); watchHandleB = watchHandleB.copyOf(n)
    }

    /** Before parking on [fd]: refuse if already cancelled, and make sure cancellation can wake us. */
    private fun watchCancellation(fd: Int, cont: Continuation<*>) {
        val job = cont.context[Job] ?: return
        if (!job.isActive) throw job.getCancellationException()
        ensureWatch(fd)
        if (watchJobA[fd] === job || watchJobB[fd] === job) return
        val handle = job.invokeOnCompletion(onCancelling = true, invokeImmediately = false) { cause ->
            // Also called when the job completes normally (cause == null): nothing is parked then, and
            // building a cancellation exception (a stack walk) per completed job cost a request's worth
            // of CPU on short-lived jobs such as withContext / withTimeout (SPEC §24.12).
            if (cause != null) postToReactor { onJobCancelled(fd, job) }
        }
        if (watchJobA[fd] == null) { watchJobA[fd] = job; watchHandleA[fd] = handle }
        else if (watchJobB[fd] == null) { watchJobB[fd] = job; watchHandleB[fd] = handle }
        else { watchHandleA[fd]?.dispose(); watchJobA[fd] = watchJobB[fd]; watchHandleA[fd] = watchHandleB[fd]; watchJobB[fd] = job; watchHandleB[fd] = handle }
    }

    /**
     * [job] was cancelled: wake its continuations parked on [fd] with CancellationException. The op
     * itself stays in flight (its buffer stays pinned) until its CQE; ops that must not complete
     * behind the caller's back (plain recv, accept, connect poll) are also cancelled in the kernel.
     */
    private fun onJobCancelled(fd: Int, job: Job) {
        val ex = job.getCancellationException()
        if (fd < readers.size) {
            val r = readers[fd]
            if (r != null && r.context[Job] === job) finishMsRead(fd, 0, ex)
        }
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.fd != fd) continue
            val c = slot.cont ?: continue
            if (c.context[Job] !== job) continue
            if (slot.sendBuf != null) { abortSend(i, slot, ex); continue }
            if (slot.readBuf != null) { abortRead(i, slot, ex); continue }
            slot.cont = null
            enqueueResumeInt(c, 0, ex)
            if (slot.cancelOnAbort) requestCancel((i.toULong() shl 32) or slot.gen.toULong())
        }
    }

    /**
     * A whole-buffer send whose writer gave up (cancelled, timed out). Resuming the writer now would
     * leave the SEND running on its buffer while a successor may already reuse it (msgtrans' INLINE
     * writer hands the buffer on), so the SEND is cancelled in the kernel and the writer is resumed
     * with [cause] from [onSendCqe], after the buffer has been advanced by what actually went out.
     */
    private fun abortSend(idx: Int, slot: Slot, cause: Throwable) {
        if (slot.sendAbort != null) return
        slot.sendAbort = cause
        requestCancel((idx.toULong() shl 32) or slot.gen.toULong())
    }

    /**
     * SPEC §23.2: wake the parked multishot reader and/or the slots on [fd] in the given direction
     * with [cause]. As with cancellation, the op stays in flight (buffers pinned) until its CQE; ops
     * whose late completion would be harmful are also cancelled in the kernel.
     */
    override fun timeoutParked(fd: Int, reads: Boolean, writes: Boolean, cause: Throwable) {
        if (reads && fd < readers.size && readers[fd] != null) finishMsRead(fd, 0, cause)
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.fd != fd || slot.multishot) continue
            if (if (slot.isWrite) !writes else !reads) continue
            val c = slot.cont ?: continue
            if (slot.sendBuf != null) { abortSend(i, slot, cause); continue }
            if (slot.readBuf != null) { abortRead(i, slot, cause); continue }
            slot.cont = null
            enqueueResumeInt(c, 0, cause)
            if (slot.cancelOnAbort) requestCancel((i.toULong() shl 32) or slot.gen.toULong())
        }
    }

    private fun forgetWatches(fd: Int) {
        if (fd >= watchJobA.size) return
        watchHandleA[fd]?.dispose(); watchHandleB[fd]?.dispose()
        watchHandleA[fd] = null; watchHandleB[fd] = null; watchJobA[fd] = null; watchJobB[fd] = null
    }

    private fun ensureFd(fd: Int) {
        if (fd < pinRefs.size) return
        var n = pinRefs.size
        while (n <= fd) n *= 2
        pinRefs = pinRefs.copyOf(n); wPinRefs = wPinRefs.copyOf(n)
    }

    private fun pinFor(fd: Int, array: ByteArray): PinRef {
        ensureFd(fd)
        val cur = pinRefs[fd]
        if (cur != null && cur.array === array) return cur
        if (cur != null) { cur.retired = true; cur.release() }
        return PinRef(array, array.pin()).also { pinRefs[fd] = it }
    }

    private fun pinForWrite(fd: Int, array: ByteArray): PinRef {
        ensureFd(fd)
        val cur = wPinRefs[fd]
        if (cur != null && cur.array === array) return cur
        if (cur != null) { cur.retired = true; cur.release() }
        return PinRef(array, array.pin()).also { wPinRefs[fd] = it }
    }

    private fun retirePin(fd: Int) {
        if (fd >= pinRefs.size) return
        pinRefs[fd]?.let { it.retired = true; it.release(); pinRefs[fd] = null }
        wPinRefs[fd]?.let { it.retired = true; it.release(); wPinRefs[fd] = null }
    }

    init {
        val depth = getenv("NETON_IO_URING_DEPTH")?.toKString()?.toUIntOrNull() ?: DEFAULT_DEPTH
        val params = nativeHeap.alloc<neton_io_uring_params>()
        // SPEC §17c step 5: the ring setup ntex/geario use. COOP_TASKRUN + DEFER_TASKRUN keep
        // completion task_work off the reactor's interrupt path (it runs inside our own
        // io_uring_enter instead), SINGLE_ISSUER tells the kernel only this thread submits.
        // Older kernels reject the flags with EINVAL: fall back to a plain ring. NETON_IO_URING_SETUP=legacy
        // forces the plain ring (A/B).
        val wantModern = getenv("NETON_IO_URING_SETUP")?.toKString() != "legacy"
        var fd = -1
        if (wantModern) {
            params.flags = (NETON_IORING_SETUP_COOP_TASKRUN or NETON_IORING_SETUP_SINGLE_ISSUER or NETON_IORING_SETUP_DEFER_TASKRUN).toUInt()
            fd = neton_uring_setup(depth, params.ptr)
        }
        if (fd < 0) {
            memset(params.ptr, 0, sizeOf<neton_io_uring_params>().convert())
            fd = neton_uring_setup(depth, params.ptr)
            modernSetup = false
        } else modernSetup = true
        ringFd = fd
        check(ringFd >= 0) { val e = platform.posix.errno; "io_uring_setup failed: ${errnoMessage(e)} (errno $e)" }

        val prot = PROT_READ or PROT_WRITE
        val flags = MAP_SHARED or MAP_POPULATE
        val sqRingBytes = (params.sq_off.array + params.sq_entries * 4u).toULong()
        val cqRingBytes = (params.cq_off.cqes + params.cq_entries * SIZEOF_CQE).toULong()
        val sqesBytes = (params.sq_entries * SIZEOF_SQE).toULong()
        sqBase = mmap(null, sqRingBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_SQ_RING.convert())!!
        cqBase = mmap(null, cqRingBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_CQ_RING.convert())!!
        sqes = mmap(null, sqesBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_SQES.convert())!!
        sqMapLen = sqRingBytes; cqMapLen = cqRingBytes; sqesMapLen = sqesBytes

        sqHeadOff = params.sq_off.head
        sqTailOff = params.sq_off.tail
        sqMaskOff = params.sq_off.ring_mask
        sqArrayOff = params.sq_off.array
        sqEntries = params.sq_entries
        cqHeadOff = params.cq_off.head
        cqTailOff = params.cq_off.tail
        cqMaskOff = params.cq_off.ring_mask
        cqesOff = params.cq_off.cqes
        nativeHeap.free(params.ptr.rawValue)

        // Provided-buffer pool for multishot recv. Registered synchronously (one blocking enter) so
        // that an unsupported kernel is detected here and reads fall back to plain recv SQEs.
        // SPEC §24.7: reads go straight into the caller's buffer by default (single RECV); multishot
        // with provided buffers is opt-in (NETON_IO_URING_MULTISHOT=1): less memory for idle
        // connections, but a copy and a CQE per 16 KB chunk (0.78-0.93 of geario at 64 KB).
        val wantMs = getenv("NETON_IO_URING_MULTISHOT")?.toKString() == "1"
        bufCount = getenv("NETON_IO_URING_BUFS")?.toKString()?.toIntOrNull() ?: DEFAULT_BUFS
        bufSize = getenv("NETON_IO_URING_BUFSZ")?.toKString()?.toIntOrNull() ?: DEFAULT_BUFSZ
        var ok = false
        if (wantMs) {
            val base = malloc((bufCount.toLong() * bufSize).convert())?.reinterpret<ByteVar>()
            if (base != null) {
                bufBase = base
                val ud = controlUd()
                prepSqe(NETON_IORING_OP_PROVIDE_BUFFERS, bufCount, base.toLong(), bufSize, 0, ud, off = 0uL, bufGroup = BUF_GROUP)
                neton_uring_enter(ringFd, pending(), 1u, NETON_IORING_ENTER_GETEVENTS.toUInt())
                // Reap by hand: the CQE for `ud` tells whether the kernel accepted the pool.
                val mask = neton_load32(cqBase, cqMaskOff)
                var head = neton_load32(cqBase, cqHeadOff)
                val tail = neton_load32(cqBase, cqTailOff)
                while (head != tail) {
                    val cqe = neton_cqe_at(cqBase, cqesOff, head and mask)!!.pointed
                    if (cqe.user_data == ud) ok = cqe.res >= 0
                    head += 1u
                }
                neton_store32(cqBase, cqHeadOff, tail)
                if (!ok) { free(base); bufBase = null }
            }
        }
        multishot = ok
        freeBufs = if (ok) bufCount else 0
    }

    private fun nowMs(): Long = reactorNowMs()

    // ---- SPEC §24.8 registered files (NETON_IO_URING_FIXED_FILES=1): a sparse table registered at
    // start; each stream's socket goes into slot == fd, so its SQEs skip the per-op file lookup.
    private val fixedCap: Int = if (getenv("NETON_IO_URING_FIXED_FILES")?.toKString() == "1")
        neton_uring_register_sparse_files(ringFd, 65536u).coerceAtLeast(0) else 0
    private val fixedFd = BooleanArray(fixedCap)

    override fun registerStream(fd: Int) {
        if (fd in 0 until fixedCap && !fixedFd[fd] && neton_uring_update_file(ringFd, fd.toUInt(), fd) >= 0) fixedFd[fd] = true
    }

    private fun pending(): UInt = neton_load32(sqBase, sqTailOff) - neton_load32(sqBase, sqHeadOff)

    private fun prepSqe(opcode: Int, fd: Int, addr: Long, len: Int, opFlags: Int, ud: ULong,
                        off: ULong = 0uL, sqeFlags: Int = 0, ioprio: Int = 0, bufGroup: Int = 0): ULong {
        // Make room if the SQ is full: submit pending SQEs so the kernel consumes them (advancing
        // sq_head). Loop because a submit may be partial; reap if the CQ is full and blocks submits.
        while (pending() >= sqEntries) {
            val n = neton_uring_enter(ringFd, pending(), 0u, 0u)
            if (n < 0) reap()
        }
        val tail = neton_load32(sqBase, sqTailOff)
        val mask = neton_load32(sqBase, sqMaskOff)
        val index = tail and mask
        val sqePtr = neton_sqe_at(sqes, index)!!
        memset(sqePtr, 0, SIZEOF_SQE.convert())
        val sqe = sqePtr.pointed
        sqe.opcode = opcode.toUByte()
        sqe.fd = fd
        sqe.addr = addr.toULong()
        sqe.len = len.toUInt()
        sqe.op_flags = opFlags.toUInt()
        sqe.off = off
        // SPEC §24.8: a registered stream socket is referenced by its table slot (slot == fd).
        var flags = sqeFlags
        if (fixedCap > 0 && fd in 0 until fixedCap && fixedFd[fd] &&
            (opcode == NETON_IORING_OP_RECV || opcode == NETON_IORING_OP_SEND || opcode == NETON_IORING_OP_SENDMSG || opcode == NETON_IORING_OP_READ)) {
            flags = flags or NETON_IOSQE_FIXED_FILE.toInt()
        }
        sqe.flags = flags.toUByte()
        sqe.ioprio = ioprio.toUShort()
        sqe.buf_index = bufGroup.toUShort()      // union with buf_group in the UAPI
        sqe.user_data = ud
        neton_array_at(sqBase, sqArrayOff, index)!!.pointed.value = index
        neton_store32(sqBase, sqTailOff, tail + 1u)
        return ud
    }

    /**
     * Submit [opcode] and suspend until its CQE. [pinned] (if any) is owned by the op from here on
     * and is released only by [reap] when the CQE arrives — also after cancellation.
     */
    /**
     * [cancelOnAbort]: whether cancelling the awaiting coroutine should also ask the kernel to
     * cancel the op (SPEC §17c step 7). Needed where a stale completion would be harmful — a plain
     * recv could consume bytes meant for a later read on the same fd, an accept would leak the
     * accepted fd, a connect poll would be re-armed twice. Not needed for SEND: a cancelled send
     * either did or did not transmit its bytes (exactly as with a lost cancel race today), the
     * buffer stays pinned until the CQE, and the CQE just resumes a cancelled continuation
     * (ignored). Skipping the handler saves a JobNode + closure per op.
     */
    private suspend fun submit(
        opcode: Int, fd: Int, addr: Long, len: Int, opFlags: Int, pin: PinRef?, cancelOnAbort: Boolean = true,
        extraPins: Array<Pinned<ByteArray>>? = null, nativeBlock: CPointer<ByteVar>? = null, isWrite: Boolean = false,
    ): Int {
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.pin = pin
        slot.extraPins = extraPins; slot.nativeBlock = nativeBlock; slot.isWrite = isWrite
        if (pin != null) pin.refs++
        val gen = slot.gen
        val ud = (idx.toULong() shl 32) or gen.toULong()
        slot.cancelOnAbort = cancelOnAbort
        return suspendCoroutineUninterceptedOrReturn { cont ->
            // Refuses to park (throws) if already cancelled — before the SQE exists, so nothing leaks.
            try { watchCancellation(fd, cont) } catch (t: Throwable) {
                if (pin != null) pin.refs--
                releaseSlot(idx, slot)
                throw t
            }
            prepSqe(opcode, fd, addr, len, opFlags, ud)
            slot.cont = cont
            COROUTINE_SUSPENDED
        }
    }

    /** Fire-and-forget IORING_OP_ASYNC_CANCEL targeting [target]; its own CQE carries no waiter. */
    private fun requestCancel(target: ULong) {
        prepSqe(NETON_IORING_OP_ASYNC_CANCEL, -1, target.toLong(), 0, 0, controlUd())
    }

    // Both branches are tail calls: a non-tail call in either made the whole function a state
    // machine, allocated on every read even on the multishot path (SPEC §24).
    override suspend fun read(fd: Int, dst: Buffer, sizer: ReadSizer): Int =
        if (multishot) readMultishot(fd, dst, sizer) else readSingle(fd, dst, sizer)

    /**
     * SPEC §24.7: one RECV straight into [dst] (pinned, no copy); the reactor commits the bytes and
     * resumes the reader from [onReadCqe]. A tail call into the intrinsic: nothing allocated.
     */
    private suspend fun readSingle(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        val cap = dst.reserve(sizer.readChunk(), bufferPool)     // may replace the backing array: pin after
        val pin = pinFor(fd, dst.backingArray())
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.pin = pin; pin.refs++
        slot.readBuf = dst; slot.readSizer = sizer; slot.cancelOnAbort = true
        slot.readState = RS_RECV; slot.parkEpoch = sweepEpoch
        if (idlePoll) idleArrays = true
        val ud = (idx.toULong() shl 32) or slot.gen.toULong()
        return suspendCoroutineUninterceptedOrReturn { cont ->
            try { watchCancellation(fd, cont) } catch (t: Throwable) { pin.refs--; releaseSlot(idx, slot); throw t }
            prepSqe(NETON_IORING_OP_RECV, fd, pin.pinned.addressOf(dst.writerIndex()).toLong(), cap, 0, ud, ioprio = recvIoprio)
            slot.cont = cont
            COROUTINE_SUSPENDED
        }
    }

    /**
     * SPEC §26.5: a single RECV parked across a whole sweep interval (or through an idle wait) is
     * cancelled; its CQE hands the buffer back and a POLL_ADD waits for data without one.
     */
    private fun demoteIdleReads(all: Boolean) {
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.readBuf == null || slot.readState != RS_RECV || slot.cont == null || slot.readAbort != null) continue
            if (all || slot.parkEpoch < sweepEpoch) {
                slot.readState = RS_DEMOTING
                requestCancel((i.toULong() shl 32) or slot.gen.toULong())
            } else idleArrays = true
        }
    }

    private fun queueRead(idx: Int, slot: Slot, state: Int) {
        slot.readState = state
        if (pendingReadCount == pendingReads.size) pendingReads = pendingReads.copyOf(pendingReadCount * 2)
        pendingReads[pendingReadCount++] = idx
    }

    /**
     * The CQE of a demoted read's RECV (cancelled: no bytes) or of its POLL_ADD. Buffer work happens
     * here; the next SQE is submitted by [advanceReads] after reaping. A reader that gave up or was
     * closed in between is finished the usual way.
     */
    private fun onIdleReadCqe(idx: Int, slot: Slot, res: Int) {
        if (slot.readState == RS_DEMOTING) {
            // The RECV did not start: the buffer is ours again. Unpin it (retire the fd's read pin so
            // the pool can hand the array to anyone) and give the array back.
            slot.pin?.let { it.refs--; it.release() }
            slot.pin = null
            if (slot.fd < pinRefs.size) pinRefs[slot.fd]?.let { it.retired = true; it.release(); pinRefs[slot.fd] = null }
            slot.readBuf!!.releaseIfIdle(bufferPool)
            stats?.let { it.idleDemotions++ }
            queueRead(idx, slot, RS_QUEUED_POLL)
        } else {                                                   // RS_POLL
            if (res < 0 && -res != ECANCELED) { finishIdleRead(idx, slot, IoException("io_uring poll failed: ${errnoMessage(-res)}", -res)); return }
            stats?.let { it.idleWakes++ }
            queueRead(idx, slot, RS_QUEUED_RECV)
        }
    }

    /** End a read that has no op in flight: resume its reader (if any) with [error] or its abort cause. */
    private fun finishIdleRead(idx: Int, slot: Slot, error: Throwable?) {
        val cont = slot.cont
        val abort = slot.readAbort
        releaseSlot(idx, slot)
        if (cont != null) enqueueResumeInt(cont, 0, abort ?: error ?: ClosedException())
    }

    /** After reaping (SPEC §26.5): arm the POLL_ADD of a demoted read, or its RECV once data is there. */
    private fun advanceReads() {
        val count = pendingReadCount
        pendingReadCount = 0
        for (i in 0 until count) {
            val idx = pendingReads[i]
            val slot = slots[idx] ?: continue
            if (!slot.live || slot.readBuf == null) continue
            if (slot.cont == null || slot.readAbort != null) { finishIdleRead(idx, slot, null); continue }
            val ud = (idx.toULong() shl 32) or slot.gen.toULong()
            if (slot.readState == RS_QUEUED_POLL) {
                slot.readState = RS_POLL
                prepSqe(NETON_IORING_OP_POLL_ADD, slot.fd, 0L, 0, NETON_POLLIN, ud)
                continue
            }
            // RS_QUEUED_RECV: data (or EOF) is waiting, so no POLL_FIRST; back on the common path.
            val dst = slot.readBuf!!
            val cap = try { dst.reserve(slot.readSizer!!.readChunk(), bufferPool) } catch (t: Throwable) { finishIdleRead(idx, slot, t); continue }
            val pin = pinFor(slot.fd, dst.backingArray())
            slot.pin = pin; pin.refs++
            slot.readState = RS_RECV; slot.parkEpoch = sweepEpoch
            idleArrays = true
            prepSqe(NETON_IORING_OP_RECV, slot.fd, pin.pinned.addressOf(dst.writerIndex()).toLong(), cap, 0, ud)
        }
    }

    /** A RECV CQE: commit what arrived into the reader's buffer, then resume it (SPEC §24.7). */
    private fun onReadCqe(idx: Int, slot: Slot, res: Int) {
        if (slot.readState != RS_RECV && (slot.readState == RS_POLL || res == -ECANCELED) && slot.readAbort == null && slot.cont != null) {
            onIdleReadCqe(idx, slot, res); return
        }
        if (slot.readState == RS_POLL) { finishIdleRead(idx, slot, null); return }  // gave up or closed while polling
        val cont = slot.cont
        val dst = slot.readBuf!!
        val sizer = slot.readSizer!!
        val abort = slot.readAbort
        slot.pin?.let { it.refs--; it.release() }
        releaseSlot(idx, slot)
        if (cont == null) return                     // stream closed under the RECV: buffer abandoned
        // Bytes that arrived before a cancel/timeout took effect are kept, never dropped.
        if (res > 0) { dst.commitWrite(res); sizer.onRead(res); stats?.let { it.reads++; it.readBytes += res } }
        when {
            abort != null -> enqueueResumeInt(cont, 0, abort)
            res > 0 -> enqueueResumeInt(cont, res)
            res == 0 -> enqueueResumeInt(cont, -1)
            else -> enqueueResumeInt(cont, 0, IoException("io_uring recv failed: ${errnoMessage(-res)}", -res))
        }
    }

    /** A single RECV whose reader gave up: cancel it in the kernel; [onReadCqe] resumes with [cause]. */
    private fun abortRead(idx: Int, slot: Slot, cause: Throwable) {
        if (slot.readAbort != null) return
        slot.readAbort = cause
        requestCancel((idx.toULong() shl 32) or slot.gen.toULong())
    }

    /**
     * Send all of [src] (SPEC §24): one SEND SQE; a short send is resubmitted by the reactor after
     * reaping, and the caller is resumed once, with the total. A tail call: nothing allocated.
     */
    override suspend fun write(fd: Int, src: Buffer): Int {
        if (src.readableBytes == 0) return 0
        val pin = pinForWrite(fd, src.backingArray())
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.pin = pin; pin.refs++
        slot.isWrite = true; slot.cancelOnAbort = false
        slot.sendBuf = src; slot.sendTotal = 0
        val ud = (idx.toULong() shl 32) or slot.gen.toULong()
        return suspendCoroutineUninterceptedOrReturn { cont ->
            try { watchCancellation(fd, cont) } catch (t: Throwable) { pin.refs--; releaseSlot(idx, slot); throw t }
            prepSqe(NETON_IORING_OP_SEND, fd, pin.pinned.addressOf(src.readerIndex()).toLong(), src.readableBytes, NETON_MSG_NOSIGNAL, ud)
            slot.cont = cont
            COROUTINE_SUSPENDED
        }
    }

    /** A send CQE: account it; resubmit the rest after reaping, or finish and resume the writer. */
    private fun onSendCqe(idx: Int, slot: Slot, res: Int) {
        stats?.let { it.writes++; if (res > 0) it.writeBytes += res }
        val cont = slot.cont
        if (cont == null) {
            // The stream was closed under the SEND: its buffer is abandoned, never touched again.
            slot.pin?.let { it.refs--; it.release() }
            releaseSlot(idx, slot)
            return
        }
        val src = slot.sendBuf!!
        if (res > 0) { src.consumeSent(res); slot.sendTotal += res }
        val abort = slot.sendAbort
        if (abort != null) {                       // cancelled / timed out: the buffer now shows what went out
            slot.pin?.let { it.refs--; it.release() }
            releaseSlot(idx, slot)
            enqueueResumeInt(cont, 0, abort)
            return
        }
        if (res >= 0 && src.readableBytes > 0) {
            if (pendingSendCount == pendingSends.size) pendingSends = pendingSends.copyOf(pendingSendCount * 2)
            pendingSends[pendingSendCount++] = idx
            return
        }
        val total = slot.sendTotal
        slot.pin?.let { it.refs--; it.release() }
        releaseSlot(idx, slot)
        if (cont == null) return
        if (res < 0) enqueueResumeInt(cont, 0, IoException("io_uring send failed: ${errnoMessage(-res)}", -res))
        else enqueueResumeInt(cont, total)
    }

    /** After reaping: resubmit the unsent rest of short sends (or drop them if the writer is gone). */
    private fun resubmitSends() {
        val count = pendingSendCount
        pendingSendCount = 0
        for (i in 0 until count) {
            val idx = pendingSends[i]
            val slot = slots[idx] ?: continue
            if (!slot.live) continue
            val src = slot.sendBuf!!
            val pin = slot.pin!!
            val abort = slot.sendAbort
            if (abort != null && slot.cont != null) {   // gave up between two SENDs: nothing in flight
                val c = slot.cont!!
                slot.cont = null
                pin.refs--; pin.release(); releaseSlot(idx, slot)
                enqueueResumeInt(c, 0, abort)
                continue
            }
            if (slot.cont == null || pin.array !== src.backingArray()) {
                slot.cont?.let { slot.cont = null; enqueueResumeInt(it, 0, IoException("write buffer changed while sending", 0)) }
                pin.refs--; pin.release(); releaseSlot(idx, slot); continue
            }
            prepSqe(NETON_IORING_OP_SEND, slot.fd, pin.pinned.addressOf(src.readerIndex()).toLong(), src.readableBytes, NETON_MSG_NOSIGNAL,
                (idx.toULong() shl 32) or slot.gen.toULong())
        }
    }

    /**
     * Vectored send (SPEC §23.3): one IORING_OP_SENDMSG per batch of up to [MAX_IOV] buffers. The
     * msghdr + iovec live in one malloc'd block and the arrays are pinned until the op's CQE.
     */
    override suspend fun writev(fd: Int, bufs: Array<Buffer>, count: Int): Long {
        var total = 0L
        var i = 0
        while (i < count && bufs[i].readableBytes == 0) i++
        while (i < count) {
            val n = minOf(MAX_IOV, count - i)
            val pins = Array(n) { bufs[i + it].backingArray().pin() }
            val block = malloc((sizeOf<msghdr>() + n * sizeOf<iovec>()).convert())!!.reinterpret<ByteVar>()
            memset(block, 0, (sizeOf<msghdr>() + n * sizeOf<iovec>()).convert())
            val msg = block.reinterpret<msghdr>()
            val iov = (block + sizeOf<msghdr>())!!.reinterpret<iovec>()
            for (k in 0 until n) {
                val b = bufs[i + k]
                iov[k].iov_base = if (b.readableBytes == 0) null else pins[k].addressOf(b.readerIndex())   // an empty pooled buffer holds a 0-length array
                iov[k].iov_len = b.readableBytes.convert()
            }
            msg.pointed.msg_iov = iov
            msg.pointed.msg_iovlen = n.convert()
            val res = submit(NETON_IORING_OP_SENDMSG, fd, msg.toLong(), 1, NETON_MSG_NOSIGNAL, null,
                cancelOnAbort = false, extraPins = pins, nativeBlock = block, isWrite = true)
            stats?.let { it.writes++; if (res > 0) it.writeBytes += res }
            total += res
            i = advanceBuffers(bufs, i, count, res.toLong())
        }
        return total
    }

    override suspend fun accept(listenFd: Int): Int {
        val res = submit(NETON_IORING_OP_ACCEPT, listenFd, 0L, 0, 0, null)
        setNonBlocking(res); suppressSigpipe(res)
        return res
    }

    override fun closeStream(fd: Int) {
        checkOwner("close")
        // Fail the awaiters now; the ops stay in flight (buffers pinned) until their CQEs arrive.
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.fd != fd) continue
            val cont = slot.cont
            if (cont == null && !slot.multishot) continue
            slot.cont = null
            // A send already being aborted keeps its cause (e.g. the writer's cancellation): the
            // writer must see the same outcome it asked for, not a closed stream.
            if (cont != null) enqueueResumeInt(cont, 0, slot.sendAbort ?: slot.readAbort ?: ClosedException())
            requestCancel((i.toULong() shl 32) or slot.gen.toULong())
        }
        if (multishot && fd < msArmed.size) {
            // The armed multishot op is cancelled above like any slot (its CQE releases the slot);
            // chunks nobody read go back to the pool; the parked reader fails like any other waiter.
            while (rqCount[fd] > 0) reprovide((rqPop(fd) shr 32).toInt())
            msEof[fd] = false; msErr[fd] = 0; rqBytes[fd] = 0; msPaused[fd] = false
            finishMsRead(fd, 0, ClosedException())
            var i = 0
            while (i < starvedCount) { if (starved[i] == fd) starved[i] = starved[--starvedCount] else i++ }
        }
        forgetWatches(fd)
        retirePin(fd)
        if (fixedCap > 0 && fd in 0 until fixedCap && fixedFd[fd]) {
            // The registered table keeps the socket alive until every request submitted before this
            // update has completed (the ring's resource node), so close() alone could leave it open:
            // no FIN, and a peer parked in a read waits forever. Shut it down explicitly first.
            platform.posix.shutdown(fd, platform.posix.SHUT_RDWR)
            neton_uring_update_file(ringFd, fd.toUInt(), -1); fixedFd[fd] = false
        }
        closeFd(fd)
    }

    override suspend fun awaitConnect(fd: Int) {
        submit(NETON_IORING_OP_POLL_ADD, fd, 0L, 0, NETON_POLLOUT, null, isWrite = true)
    }

    private var wakeUd: ULong = 0uL

    private fun armWake() { wakeUd = prepSqe(NETON_IORING_OP_POLL_ADD, wakeReadFd, 0L, 0, NETON_POLLIN, controlUd()) }

    // One IORING_OP_TIMEOUT at a time bounds the wait to the nearest timer. The timespec must
    // stay valid until the op completes, so it lives on the native heap for the reactor's life.
    private val timeoutSpec = nativeHeap.alloc<neton_kernel_timespec>()
    private var timeoutUd: ULong = 0uL
    private var timeoutDeadlineMs = Long.MAX_VALUE

    private fun armTimeout(ms: Int) {
        timeoutSpec.tv_sec = (ms / 1000).toLong()
        timeoutSpec.tv_nsec = ((ms % 1000) * 1_000_000).toLong()
        timeoutUd = prepSqe(NETON_IORING_OP_TIMEOUT, -1, timeoutSpec.ptr.toLong(), 1, 0, controlUd())
    }

    override fun runUntil(root: Job) {
        armWake()
        while (!root.isCompleted) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (root.isCompleted) break

            var timerMs = nextTimerMillis()
            val sweepWait = idleArrays && !hasTasks() && (timerMs < 0 || timerMs > IDLE_SWEEP_MS)
            if (sweepWait) timerMs = IDLE_SWEEP_MS
            val mc = if (hasTasks() || timerMs == 0) 0u else 1u
            if (mc > 0u && timerMs > 0) {
                // Bound the blocking wait by the nearest timer (re-arm only if none is in flight
                // or the in-flight one would fire too late; a stale one completes harmlessly).
                val deadline = nowMs() + timerMs
                if (timeoutUd == 0uL || deadline < timeoutDeadlineMs) { armTimeout(timerMs); timeoutDeadlineMs = deadline }
            }
            val toSubmit = pending()
            // GETEVENTS on every enter, also when not blocking (min_complete 0 returns at once):
            // under DEFER_TASKRUN completion task_work only runs inside such an enter, and a loop
            // that always has tasks pending would otherwise never see its CQEs.
            neton_uring_enter(ringFd, toSubmit, mc, NETON_IORING_ENTER_GETEVENTS.toUInt())
            countPoll(mc == 0u, reap())
            // SPEC §24: work that submits SQEs runs here, never from inside reap().
            if (msReadyCount > 0) completeReadyReads()
            if (pendingSendCount > 0) resubmitSends()
            if (pendingReadCount > 0) advanceReads()
            if (sweepWait && timeoutFired) sweepParked(all = true)
            else if (++roundsSinceSweep >= SWEEP_ROUNDS && idleArrays) sweepParked(all = false)
            timeoutFired = false
        }
    }

    /** Consume every CQE in the ring; returns how many were reaped. */
    private fun reap(): Int {
        val mask = neton_load32(cqBase, cqMaskOff)
        var head = neton_load32(cqBase, cqHeadOff)
        val tail = neton_load32(cqBase, cqTailOff)
        val n = (tail - head).toInt()
        while (head != tail) {
            val cqe = neton_cqe_at(cqBase, cqesOff, head and mask)!!.pointed
            val ud = cqe.user_data
            val res = cqe.res
            head += 1u
            if (ud >= CONTROL_BASE) {
                if (ud == wakeUd) { onWake(); armWake() }
                else if (ud == timeoutUd) { timeoutUd = 0uL; timeoutDeadlineMs = Long.MAX_VALUE; timeoutFired = true }
                continue                                   // else: a cancel op's own CQE
            }
            val idx = (ud shr 32).toInt()
            val slot = slots.getOrNull(idx) ?: continue
            if (!slot.live || slot.gen != ud.toUInt()) continue   // stale generation: unknown op
            if (slot.multishot) { onMultishotCqe(idx, slot, res, cqe.flags); continue }
            if (slot.sendBuf != null) { onSendCqe(idx, slot, res); continue }
            if (slot.readBuf != null) { onReadCqe(idx, slot, res); continue }
            slot.pin?.let { it.refs--; it.release() }      // the kernel is done with the buffer
            val cont = slot.cont                           // null if the awaiter was cancelled/closed
            releaseSlot(idx, slot)
            if (cont == null) continue
            if (res < 0) enqueueResumeInt(cont, 0, IoException("io_uring op failed: ${errnoMessage(-res)}", -res))
            else enqueueResumeInt(cont, res)
        }
        neton_store32(cqBase, cqHeadOff, tail)
        return n
    }

    /**
     * Drain before closing: cancel every op still in flight and reap until their CQEs arrive, so
     * no buffer is released while the kernel may still touch it. Closing the ring with ops in flight
     * would cancel them asynchronously, after close() returns. If the drain does not converge the
     * remaining buffers stay pinned (a leak, reported), never a use-after-free.
     */
    override fun shutdown() {
        // Short sends waiting for resubmission have no op in flight: release them first.
        for (i in 0 until pendingSendCount) {
            val slot = slots[pendingSends[i]] ?: continue
            if (slot.live) { slot.cont = null; slot.pin?.let { it.refs--; it.release() }; releaseSlot(pendingSends[i], slot) }
        }
        pendingSendCount = 0
        for (i in 0 until pendingReadCount) {
            val slot = slots[pendingReads[i]] ?: continue
            if (slot.live) { slot.cont = null; releaseSlot(pendingReads[i], slot) }
        }
        pendingReadCount = 0
        if (liveOps > 0) {
            for (i in slots.indices) {
                val slot = slots[i] ?: continue
                if (slot.live) { slot.cont = null; requestCancel((i.toULong() shl 32) or slot.gen.toULong()) }
            }
            var rounds = 0
            while (liveOps > 0 && rounds < DRAIN_ROUNDS) {
                neton_uring_enter(ringFd, pending(), 1u, NETON_IORING_ENTER_GETEVENTS.toUInt())
                reap()
                rounds++
            }
            if (liveOps > 0) {
                fprintf(stderr, "neton-io: %d io_uring op(s) did not complete after cancel; buffers left pinned\n", liveOps)
                // slots dropped, pins intentionally kept
            }
        }
        close(ringFd)
        // Release everything init acquired: the three ring mappings and the timeout spec.
        // The ring fd is closed first, so the kernel is done with these pages.
        munmap(sqes, sqesMapLen.convert())
        munmap(cqBase, cqMapLen.convert())
        munmap(sqBase, sqMapLen.convert())
        nativeHeap.free(timeoutSpec.ptr.rawValue)
        bufBase?.let { free(it) }
        closeWakePipe()
    }

    private companion object {
        const val DEFAULT_DEPTH: UInt = 4096u
        const val BUF_GROUP = 1
        const val DEFAULT_BUFS = 512
        const val DEFAULT_BUFSZ = 16 * 1024
        const val SIZEOF_SQE: UInt = 64u
        const val SIZEOF_CQE: UInt = 16u
        const val DRAIN_ROUNDS = 1000
        /** [msTake]: nothing queued, no EOF, no error — the reader must wait. */
        const val NOTHING_QUEUED = Int.MIN_VALUE
        const val IORING_RECVSEND_POLL_FIRST = 1
        const val IDLE_SWEEP_MS = 50
        // SPEC §26.5 single-read states: RECV in flight; its cancel requested by the idle sweep;
        // POLL_ADD in flight (no buffer); waiting in pendingReads to submit the POLL_ADD / the RECV.
        const val RS_RECV = 0
        const val RS_DEMOTING = 1
        const val RS_POLL = 2
        const val RS_QUEUED_POLL = 3
        const val RS_QUEUED_RECV = 4
        const val SWEEP_ROUNDS = 1024
        /** user_data at or above this belongs to a control op (wake poll / timeout / cancel), not a slot. */
        const val CONTROL_BASE: ULong = 0xFFFF_FFFF_0000_0000uL
    }
}
