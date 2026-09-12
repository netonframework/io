package neton.io.net

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
import kotlinx.cinterop.toKString
import kotlinx.cinterop.toLong
import kotlinx.cinterop.value
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Job
import kotlinx.coroutines.suspendCancellableCoroutine
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import kotlin.coroutines.resumeWithException
import neton.io.uring.NETON_IORING_ENTER_GETEVENTS
import neton.io.uring.NETON_IORING_OFF_CQ_RING
import neton.io.uring.NETON_IORING_OFF_SQES
import neton.io.uring.NETON_IORING_OFF_SQ_RING
import neton.io.uring.NETON_IORING_OP_ACCEPT
import neton.io.uring.NETON_IORING_OP_ASYNC_CANCEL
import neton.io.uring.NETON_IORING_OP_POLL_ADD
import neton.io.uring.NETON_IORING_OP_READ
import neton.io.uring.NETON_IORING_OP_SEND
import neton.io.uring.NETON_MSG_NOSIGNAL
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
import platform.posix.mmap
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
@OptIn(ExperimentalForeignApi::class)
internal class UringReactor : Reactor() {

    private val ringFd: Int
    private val sqBase: COpaquePointer
    private val cqBase: COpaquePointer
    private val sqes: COpaquePointer

    private val sqHeadOff: UInt
    private val sqTailOff: UInt
    private val sqMaskOff: UInt
    private val sqArrayOff: UInt
    private val sqEntries: UInt
    private val cqHeadOff: UInt
    private val cqTailOff: UInt
    private val cqMaskOff: UInt
    private val cqesOff: UInt

    private var nextUserData: ULong = 1uL

    override val driverName: String get() = "iouring"

    /** One submitted op: its pinned buffer (if any) and the continuation awaiting it (null once cancelled). */
    private class InFlight(val fd: Int, val pinned: Pinned<ByteArray>?, var cont: CancellableContinuation<Int>?)

    private val inFlight = HashMap<ULong, InFlight>()

    init {
        val depth = getenv("NETON_IO_URING_DEPTH")?.toKString()?.toUIntOrNull() ?: DEFAULT_DEPTH
        val params = nativeHeap.alloc<neton_io_uring_params>()
        ringFd = neton_uring_setup(depth, params.ptr)
        check(ringFd >= 0) { "io_uring_setup failed (fd=$ringFd)" }

        val prot = PROT_READ or PROT_WRITE
        val flags = MAP_SHARED or MAP_POPULATE
        val sqRingBytes = (params.sq_off.array + params.sq_entries * 4u).toULong()
        val cqRingBytes = (params.cq_off.cqes + params.cq_entries * SIZEOF_CQE).toULong()
        val sqesBytes = (params.sq_entries * SIZEOF_SQE).toULong()
        sqBase = mmap(null, sqRingBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_SQ_RING.convert())!!
        cqBase = mmap(null, cqRingBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_CQ_RING.convert())!!
        sqes = mmap(null, sqesBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_SQES.convert())!!

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
    }

    private fun pending(): UInt = neton_load32(sqBase, sqTailOff) - neton_load32(sqBase, sqHeadOff)

    private fun prepSqe(opcode: Int, fd: Int, addr: Long, len: Int, opFlags: Int): ULong {
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
        val ud = nextUserData++
        sqe.user_data = ud
        neton_array_at(sqBase, sqArrayOff, index)!!.pointed.value = index
        neton_store32(sqBase, sqTailOff, tail + 1u)
        return ud
    }

    /**
     * Submit [opcode] and suspend until its CQE. [pinned] (if any) is owned by the op from here on
     * and is released only by [reap] when the CQE arrives — also after cancellation.
     */
    private suspend fun submit(opcode: Int, fd: Int, addr: Long, len: Int, opFlags: Int, pinned: Pinned<ByteArray>?): Int {
        val ud = prepSqe(opcode, fd, addr, len, opFlags)
        return suspendCancellableCoroutine { cont ->
            val entry = InFlight(fd, pinned, cont)
            inFlight[ud] = entry
            cont.invokeOnCancellation {
                // Drop the waiter but keep the entry (and its pin): the kernel still owns the buffer
                // until this op's CQE. Ask the kernel to cancel; the CQE tells us when it is over.
                entry.cont = null
                if (inFlight[ud] === entry) requestCancel(ud)
            }
        }
    }

    /** Fire-and-forget IORING_OP_ASYNC_CANCEL targeting [target]; its own CQE carries no waiter. */
    private fun requestCancel(target: ULong) {
        prepSqe(NETON_IORING_OP_ASYNC_CANCEL, -1, target.toLong(), 0, 0)
    }

    override suspend fun read(fd: Int, dst: Buffer, chunk: Int): Int {
        val cap = dst.reserve(chunk)
        val pinned = dst.backingArray().pin()
        val res = submit(NETON_IORING_OP_READ, fd, pinned.addressOf(dst.writerIndex()).toLong(), cap, 0, pinned)
        stats?.let { it.reads++; if (res > 0) it.readBytes += res }
        return if (res > 0) { dst.commitWrite(res); res } else -1 // 0 = EOF (errors throw)
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        var total = 0
        while (src.readableBytes > 0) {
            val len = src.readableBytes
            val pinned = src.backingArray().pin()
            val res = submit(NETON_IORING_OP_SEND, fd, pinned.addressOf(src.readerIndex()).toLong(), len, NETON_MSG_NOSIGNAL, pinned)
            stats?.let { it.writes++; if (res > 0) it.writeBytes += res }
            src.consume(res); total += res
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
        for ((ud, entry) in inFlight) {
            if (entry.fd != fd || entry.cont == null) continue
            val cont = entry.cont!!
            entry.cont = null
            cont.resumeWithException(ClosedException())
            requestCancel(ud)
        }
        closeFd(fd)
    }

    override suspend fun awaitConnect(fd: Int) {
        submit(NETON_IORING_OP_POLL_ADD, fd, 0L, 0, NETON_POLLOUT, null)
    }

    private var wakeUd: ULong = 0uL

    private fun armWake() { wakeUd = prepSqe(NETON_IORING_OP_POLL_ADD, wakeReadFd, 0L, 0, NETON_POLLIN) }

    override fun runUntil(root: Job) {
        armWake()
        while (!root.isCompleted) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (root.isCompleted) break

            val toSubmit = pending()
            val timerMs = nextTimerMillis()
            val minComplete = if (hasTasks() || timerMs == 0) 0u else 1u
            // Timers: io_uring_enter has no timeout argument in this minimal binding; a due-soon
            // timer is honoured by a bounded wait through IORING_OP_TIMEOUT in the full binding.
            // Until then a pending timer makes the wait non-blocking and the loop polls the clock.
            val mc = if (timerMs > 0) 0u else minComplete
            val flags = if (mc > 0u) NETON_IORING_ENTER_GETEVENTS.toUInt() else 0u
            neton_uring_enter(ringFd, toSubmit, mc, flags)
            countPoll(mc == 0u, reap())
            if (mc == 0u && timerMs > 0 && !hasTasks()) platform.posix.usleep(minOf(timerMs, 1) .toUInt() * 1000u)
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
            if (ud == wakeUd) { onWake(); armWake(); continue }
            val entry = inFlight.remove(ud) ?: continue // a cancel op's own CQE, or unknown
            entry.pinned?.unpin()                          // the kernel is done with the buffer
            val cont = entry.cont ?: continue              // null if the awaiter was cancelled/closed
            if (res < 0) cont.resumeWithException(IoException("io_uring op failed: ${errnoMessage(-res)}", -res))
            else cont.resume(res)
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
        if (inFlight.isNotEmpty()) {
            for ((ud, entry) in inFlight) { entry.cont = null; requestCancel(ud) }
            var rounds = 0
            while (inFlight.isNotEmpty() && rounds < DRAIN_ROUNDS) {
                neton_uring_enter(ringFd, pending(), 1u, NETON_IORING_ENTER_GETEVENTS.toUInt())
                reap()
                rounds++
            }
            if (inFlight.isNotEmpty()) {
                fprintf(stderr, "neton-io: %d io_uring op(s) did not complete after cancel; buffers left pinned\n", inFlight.size)
                inFlight.clear() // entries dropped, pins intentionally kept
            }
        }
        close(ringFd)
        closeWakePipe()
    }

    private companion object {
        const val DEFAULT_DEPTH: UInt = 4096u
        const val SIZEOF_SQE: UInt = 64u
        const val SIZEOF_CQE: UInt = 16u
        const val DRAIN_ROUNDS = 1000
    }
}
