package neton.io.net

import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.pin
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.toLong
import kotlinx.cinterop.value
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Job
import kotlinx.coroutines.suspendCancellableCoroutine
import neton.io.bytes.Buffer
import neton.io.uring.NETON_IORING_ENTER_GETEVENTS
import neton.io.uring.NETON_IORING_OFF_CQ_RING
import neton.io.uring.NETON_IORING_OFF_SQES
import neton.io.uring.NETON_IORING_OFF_SQ_RING
import neton.io.uring.NETON_IORING_OP_ACCEPT
import neton.io.uring.NETON_IORING_OP_POLL_ADD
import neton.io.uring.NETON_IORING_OP_READ
import neton.io.uring.NETON_IORING_OP_WRITE
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
import platform.posix.memset
import platform.posix.mmap
import kotlin.coroutines.resume

/**
 * Completion reactor backed by io_uring: a read/write/accept submits the op (with its buffer)
 * and suspends until the completion carries the result. Single-threaded — submit and reap
 * happen on one thread around io_uring_enter, which is a full barrier, so no SMP ring barriers
 * are needed.
 */
@OptIn(ExperimentalForeignApi::class)
internal class UringReactor : Reactor() {

    private val ringFd: Int
    private val sqBase: COpaquePointer
    private val cqBase: COpaquePointer
    private val sqes: COpaquePointer

    private val sqTailOff: UInt
    private val sqMaskOff: UInt
    private val sqArrayOff: UInt
    private val cqHeadOff: UInt
    private val cqTailOff: UInt
    private val cqMaskOff: UInt
    private val cqesOff: UInt

    private var nextUserData: ULong = 1uL
    private var toSubmit: Int = 0
    private val waiters = HashMap<ULong, CancellableContinuation<Int>>()

    init {
        val params = nativeHeap.alloc<neton_io_uring_params>()
        ringFd = neton_uring_setup(QUEUE_DEPTH, params.ptr)
        check(ringFd >= 0) { "io_uring_setup failed (fd=$ringFd)" }

        val prot = PROT_READ or PROT_WRITE
        val flags = MAP_SHARED or MAP_POPULATE
        val sqRingBytes = (params.sq_off.array + params.sq_entries * 4u).toULong()
        val cqRingBytes = (params.cq_off.cqes + params.cq_entries * SIZEOF_CQE).toULong()
        val sqesBytes = (params.sq_entries * SIZEOF_SQE).toULong()
        sqBase = mmap(null, sqRingBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_SQ_RING.convert())!!
        cqBase = mmap(null, cqRingBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_CQ_RING.convert())!!
        sqes = mmap(null, sqesBytes.convert(), prot, flags, ringFd, NETON_IORING_OFF_SQES.convert())!!

        sqTailOff = params.sq_off.tail
        sqMaskOff = params.sq_off.ring_mask
        sqArrayOff = params.sq_off.array
        cqHeadOff = params.cq_off.head
        cqTailOff = params.cq_off.tail
        cqMaskOff = params.cq_off.ring_mask
        cqesOff = params.cq_off.cqes
        nativeHeap.free(params.ptr.rawValue)
    }

    private fun prepSqe(opcode: Int, fd: Int, addr: Long, len: Int, opFlags: Int): ULong {
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
        toSubmit++
        return ud
    }

    private suspend fun await(ud: ULong): Int = suspendCancellableCoroutine { cont ->
        waiters[ud] = cont
        cont.invokeOnCancellation { waiters.remove(ud) }
    }

    override suspend fun read(fd: Int, dst: Buffer, chunk: Int): Int {
        val cap = dst.reserve(chunk)
        val pinned = dst.backingArray().pin()
        val res = try {
            await(prepSqe(NETON_IORING_OP_READ, fd, pinned.addressOf(dst.writerIndex()).toLong(), cap, 0))
        } finally {
            pinned.unpin()
        }
        return if (res > 0) { dst.commitWrite(res); res } else -1 // 0 = EOF, negative = error
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        var total = 0
        while (src.readableBytes > 0) {
            val len = src.readableBytes
            val pinned = src.backingArray().pin()
            val res = try {
                await(prepSqe(NETON_IORING_OP_WRITE, fd, pinned.addressOf(src.readerIndex()).toLong(), len, 0))
            } finally {
                pinned.unpin()
            }
            if (res > 0) { src.consume(res); total += res } else break
        }
        return total
    }

    override suspend fun accept(listenFd: Int): Int {
        val res = await(prepSqe(NETON_IORING_OP_ACCEPT, listenFd, 0L, 0, 0))
        if (res >= 0) setNonBlocking(res)
        return res
    }

    override suspend fun awaitConnect(fd: Int) {
        await(prepSqe(NETON_IORING_OP_POLL_ADD, fd, 0L, 0, NETON_POLLOUT))
    }

    override fun runUntil(root: Job) {
        while (!root.isCompleted) {
            drainTasks()
            if (root.isCompleted) break
            if (waiters.isEmpty() && !hasTasks()) break

            val minComplete = if (hasTasks()) 0u else 1u
            val submit = toSubmit.toUInt()
            toSubmit = 0
            val enterFlags = if (minComplete > 0u) NETON_IORING_ENTER_GETEVENTS.toUInt() else 0u
            neton_uring_enter(ringFd, submit, minComplete, enterFlags)
            reap()
        }
    }

    private fun reap() {
        val mask = neton_load32(cqBase, cqMaskOff)
        var head = neton_load32(cqBase, cqHeadOff)
        val tail = neton_load32(cqBase, cqTailOff)
        while (head != tail) {
            val cqe = neton_cqe_at(cqBase, cqesOff, head and mask)!!.pointed
            val ud = cqe.user_data
            val res = cqe.res
            head += 1u
            waiters.remove(ud)?.resume(res)
        }
        neton_store32(cqBase, cqHeadOff, tail)
    }

    override fun shutdown() {
        close(ringFd)
    }

    private companion object {
        const val QUEUE_DEPTH: UInt = 256u
        const val SIZEOF_SQE: UInt = 64u
        const val SIZEOF_CQE: UInt = 16u
    }
}
