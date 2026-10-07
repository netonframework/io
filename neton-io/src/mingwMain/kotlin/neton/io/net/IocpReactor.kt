@file:OptIn(ExperimentalForeignApi::class, InternalCoroutinesApi::class)

package neton.io.net

import kotlinx.cinterop.convert
import kotlinx.cinterop.plus
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import platform.posix.free
import platform.posix.malloc
import platform.posix.memcpy
import neton.io.core.boxedInt
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.COpaquePointerVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.get
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.pin
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import neton.io.win.neton_poll_writable
import neton.io.win.neton_udp_peek
import neton.io.win.neton_accept
import neton.io.win.neton_accept_discard
import neton.io.win.neton_accept_finish
import neton.io.win.neton_cancel
import neton.io.win.neton_connect_done
import neton.io.win.neton_free
import neton.io.win.neton_iocp_associate
import neton.io.win.neton_iocp_close
import neton.io.win.neton_iocp_create
import neton.io.win.neton_iocp_post
import neton.io.win.neton_iocp_wait
import neton.io.win.neton_op_bytes
import neton.io.win.neton_op_free
import neton.io.win.neton_op_gen
import neton.io.win.neton_op_new
import neton.io.win.neton_op_set_id
import neton.io.win.neton_op_slot
import neton.io.win.neton_recv
import neton.io.win.neton_send
import platform.posix.fprintf
import platform.posix.getenv
import platform.posix.stderr
import kotlinx.cinterop.toKString
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

/**
 * Completion reactor on an I/O completion port (SPEC §22, §23.1): the Windows counterpart of
 * [UringReactor]. A read/write/accept starts an overlapped operation (WSARecv / WSASend / AcceptEx)
 * and suspends until its completion packet carries the result.
 *
 * - Sockets are associated with the port on first use, with FILE_SKIP_COMPLETION_PORT_ON_SUCCESS
 *   where the stack allows it: an operation that finishes at once returns its result directly, no
 *   packet follows, and the coroutine does not suspend (NETON_IO_IOCP_SKIP=0 turns this off).
 * - Buffer lifetime is the io_uring rule: a buffer stays pinned from the start of an operation until
 *   its completion packet, also when the awaiting coroutine is cancelled or the stream closed
 *   (CancelIoEx makes the packet arrive early, with an abort status).
 * - Writes are non-blocking sends; only a send that would block goes out overlapped, from a copy the
 *   reactor owns, and is never cancelled for its writer (SPEC §33.3: Windows cannot cancel a TCP send exactly).
 * - Cross-thread wakeups are PostQueuedCompletionStatus packets; there is no wake socket pair.
 * - Fairness (§19.5): a connection whose read completed at once is served once per loop round; its
 *   next read in the same round waits for the next round.
 * - connect has no overlapped form here (ConnectEx would need the connect path restructured), so
 *   [awaitConnect] checks for completion with a zero-timeout WSAPoll on a short backoff.
 */
@OptIn(ExperimentalAtomicApi::class)
internal class IocpReactor : Reactor() {

    override val driverName: String get() = if (useSkip) "iocp+skip" else "iocp"

    private val port: COpaquePointer = neton_iocp_create() ?: error("CreateIoCompletionPort failed")
    private val useSkip: Boolean = getenv("NETON_IO_IOCP_SKIP")?.toKString() != "0"

    // ---- per-socket state, indexed by handle / 4 (SOCKET values are multiples of 4).
    private var associated = BooleanArray(256)
    private var skipOn = BooleanArray(256)
    private var servedRound = LongArray(256)
    private var pollAccept = BooleanArray(256)          // listener where AcceptEx is unsupported (AF_UNIX)
    private var pinRefs = arrayOfNulls<PinRef>(256)
    private var wPinRefs = arrayOfNulls<PinRef>(256)
    private var watchJobA = arrayOfNulls<Job>(256); private var watchHandleA = arrayOfNulls<DisposableHandle>(256)
    private var watchJobB = arrayOfNulls<Job>(256); private var watchHandleB = arrayOfNulls<DisposableHandle>(256)

    private fun ix(fd: Int): Int {
        val i = fd ushr 2
        if (i >= associated.size) {
            var n = associated.size
            while (n <= i) n *= 2
            associated = associated.copyOf(n); skipOn = skipOn.copyOf(n); servedRound = servedRound.copyOf(n)
            pollAccept = pollAccept.copyOf(n); pinRefs = pinRefs.copyOf(n); wPinRefs = wPinRefs.copyOf(n)
            watchJobA = watchJobA.copyOf(n); watchHandleA = watchHandleA.copyOf(n)
            watchJobB = watchJobB.copyOf(n); watchHandleB = watchHandleB.copyOf(n)
            readBusy = readBusy.copyOf(n); readDeferred = readDeferred.copyOf(n); writeBusy = writeBusy.copyOf(n)
            deferredReader = deferredReader.copyOf(n); staged = staged.copyOf(n)
        }
        return i
    }

    // SPEC §28.6: one read and one write per socket at a time. Set when an op parks (slot or the
    // next-round wait), cleared when its slot is released or the wait ends; writev clears on return.
    private var readBusy = BooleanArray(256)
    private var readDeferred = BooleanArray(256)
    private var deferredReader = arrayOfNulls<Continuation<Unit>>(256)   // the reader deferred to the next round, per fd
    private var writeBusy = BooleanArray(256)
    private var staged = IntArray(256)                  // the socket's staged send: slot index + 1, 0 if none

    /** Associate [fd] with the port once; returns whether skip-on-success is active for it. */
    private fun ensureAssociated(fd: Int): Boolean {
        val i = ix(fd)
        if (associated[i]) return skipOn[i]
        val skip = nativeHeap.alloc<IntVar>()
        try {
            val rc = neton_iocp_associate(port, fd.toSocket(), if (useSkip) 1 else 0, skip.ptr)
            if (rc != 0) throw IoException("CreateIoCompletionPort(socket) failed: Win32 error $rc", rc)
            associated[i] = true
            skipOn[i] = skip.value != 0
            return skipOn[i]
        } finally {
            nativeHeap.free(skip.ptr.rawValue)
        }
    }

    override fun registerStream(fd: Int) { ensureAssociated(fd) }

    // ---- pins: as in UringReactor, one per fd for the array last used on it, ref-counted by the ops holding it.
    private class PinRef(val array: ByteArray, val pinned: Pinned<ByteArray>) { var refs = 0; var retired = false
        fun release() { if (retired && refs == 0) pinned.unpin() } }

    private fun pinFor(fd: Int, array: ByteArray): PinRef {
        val i = ix(fd)
        val cur = pinRefs[i]
        if (cur != null && cur.array === array) return cur
        if (cur != null) { cur.retired = true; cur.release() }
        return PinRef(array, array.pin()).also { pinRefs[i] = it }
    }

    private fun pinForWrite(fd: Int, array: ByteArray): PinRef {
        val i = ix(fd)
        val cur = wPinRefs[i]
        if (cur != null && cur.array === array) return cur
        if (cur != null) { cur.retired = true; cur.release() }
        return PinRef(array, array.pin()).also { wPinRefs[i] = it }
    }

    private fun retirePin(fd: Int) {
        val i = ix(fd)
        pinRefs[i]?.let { it.retired = true; it.release(); pinRefs[i] = null }
        wPinRefs[i]?.let { it.retired = true; it.release(); wPinRefs[i] = null }
    }

    // ---- in-flight operations: a slot table; each slot owns one native neton_op (OVERLAPPED first).
    private class Slot(val op: COpaquePointer) {
        var live = false; var gen = 0u; var fd = -1; var kind = 0
        var pin: PinRef? = null; var cont: Continuation<Int>? = null
        var cancelOnAbort = true; var isWrite = false
        var extraPins: Array<Pinned<ByteArray>>? = null; var nativeBlock: COpaquePointer? = null
        // SPEC §24 (as in UringReactor): a read or whole-buffer send completed by the reactor, and the
        // cause a cancelled / timed-out one is resumed with once its completion has been accounted.
        var readBuf: Buffer? = null; var readSizer: ReadSizer? = null
        var abort: Throwable? = null
        // A staged send (SPEC §33.3): its length and how much the kernel has taken; the copy is [nativeBlock].
        var stage = 0; var stageOff = 0
    }
    private var slots = arrayOfNulls<Slot>(256)
    private var freeSlots = IntArray(256) { it }
    private var freeTop = 256
    private var liveOps = 0

    private fun takeSlot(): Int {
        if (freeTop == 0) {
            val n = slots.size
            slots = slots.copyOf(n * 2)
            freeSlots = IntArray(n * 2)
            for (i in 0 until n) freeSlots[i] = n + i
            freeTop = n
        }
        val idx = freeSlots[--freeTop]
        val slot = slots[idx] ?: Slot(neton_op_new() ?: error("out of memory")).also { slots[idx] = it }
        slot.live = true; slot.gen++
        neton_op_set_id(slot.op, idx.toUInt(), slot.gen)
        liveOps++
        return idx
    }

    private fun releaseSlot(idx: Int, slot: Slot) {
        if (slot.readBuf != null) ix(slot.fd).let { readBusy[it] = false }
        slot.live = false; slot.cont = null; slot.fd = -1; slot.kind = 0; slot.isWrite = false
        slot.readBuf = null; slot.readSizer = null; slot.abort = null; slot.stage = 0; slot.stageOff = 0
        slot.pin?.let { it.refs--; it.release() }; slot.pin = null
        slot.extraPins?.let { for (p in it) p.unpin() }; slot.extraPins = null
        slot.nativeBlock?.let { neton_free(it) }; slot.nativeBlock = null
        freeSlots[freeTop++] = idx
        liveOps--
    }

    // ---- cancellation: watched once per (fd, Job), as in UringReactor.
    private fun watchCancellation(fd: Int, cont: Continuation<*>) {
        val job = cont.context[Job] ?: return
        if (!job.isActive) throw job.getCancellationException()
        val i = ix(fd)
        if (watchJobA[i] === job || watchJobB[i] === job) return
        val handle = job.invokeOnCompletion(onCancelling = true, invokeImmediately = false) { cause ->
            // Also called when the job completes normally (cause == null): nothing is parked then, and
            // building a cancellation exception (a stack walk) per completed job cost a request's worth
            // of CPU on short-lived jobs such as withContext / withTimeout (SPEC §24.12).
            if (cause != null) postToReactor { onJobCancelled(fd, job) }
        }
        // SPEC §27.9: a cancel between the isActive check and the registration calls no handler.
        if (!job.isActive) { handle.dispose(); throw job.getCancellationException() }
        if (watchJobA[i] == null) { watchJobA[i] = job; watchHandleA[i] = handle }
        else if (watchJobB[i] == null) { watchJobB[i] = job; watchHandleB[i] = handle }
        else { watchHandleA[i]?.dispose(); watchJobA[i] = watchJobB[i]; watchHandleA[i] = watchHandleB[i]; watchJobB[i] = job; watchHandleB[i] = handle }
    }

    private fun forgetWatches(fd: Int) {
        val i = ix(fd)
        watchHandleA[i]?.dispose(); watchHandleB[i]?.dispose()
        watchHandleA[i] = null; watchHandleB[i] = null; watchJobA[i] = null; watchJobB[i] = null
    }

    /** Wake [job]'s operations parked on [fd]; the ops themselves finish (or abort) with their packet. */
    private fun onJobCancelled(fd: Int, job: Job) {
        val ex = job.getCancellationException()
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.fd != fd) continue
            val c = slot.cont ?: continue
            if (c.context[Job] !== job) continue
            if (slot.readBuf != null) { abortOp(slot, ex); continue }
            slot.cont = null
            enqueueResumeInt(c, 0, ex)
            if (slot.cancelOnAbort) neton_cancel(fd.toSocket(), slot.op)
        }
    }

    override fun timeoutParked(fd: Int, reads: Boolean, writes: Boolean, cause: Throwable) {
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.fd != fd) continue
            if (if (slot.isWrite) !writes else !reads) continue
            val c = slot.cont ?: continue
            if (slot.readBuf != null) { abortOp(slot, cause); continue }
            slot.cont = null
            enqueueResumeInt(c, 0, cause)
            if (slot.cancelOnAbort) neton_cancel(fd.toSocket(), slot.op)
        }
    }

    /**
     * Start an overlapped op in a fresh slot via [start] (given the op and whether skip-on-success
     * is on) and return its result: at once if it completed synchronously, else after its packet.
     */
    private suspend inline fun submit(
        fd: Int, kind: Int, pin: PinRef?, cancelOnAbort: Boolean, isWrite: Boolean,
        extraPins: Array<Pinned<ByteArray>>? = null, nativeBlock: COpaquePointer? = null,
        crossinline start: (op: COpaquePointer, skip: Int) -> Int,
    ): Int {
        val skip = if (ensureAssociated(fd)) 1 else 0
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.kind = kind; slot.cancelOnAbort = cancelOnAbort; slot.isWrite = isWrite
        slot.pin = pin; if (pin != null) pin.refs++
        slot.extraPins = extraPins; slot.nativeBlock = nativeBlock
        return suspendCoroutineUninterceptedOrReturn { cont ->
            // Refuses (throws) if already cancelled — before the op exists, so nothing is in flight.
            try { watchCancellation(fd, cont) } catch (t: Throwable) { releaseSlot(idx, slot); throw t }
            when (val rc = start(slot.op, skip)) {
                0 -> {                                     // done now; no packet will follow
                    val n = neton_op_bytes(slot.op).toInt()
                    val r = if (kind == KIND_ACCEPT) finishAccept(fd, slot) else n
                    releaseSlot(idx, slot)
                    r
                }
                1 -> { slot.cont = cont; COROUTINE_SUSPENDED }
                else -> {
                    if (kind == KIND_ACCEPT) neton_accept_discard(slot.op)
                    releaseSlot(idx, slot)
                    throw IoException("${KIND_NAMES[kind]} failed: ${errnoMessage(rc)}", rc)
                }
            }
        }
    }

    private fun finishAccept(listenFd: Int, slot: Slot): Int {
        val s = neton_accept_finish(listenFd.toSocket(), slot.op)
        if (s == 0uL) throw IoException("accept failed: SO_UPDATE_ACCEPT_CONTEXT", 0)
        return s.toFd()
    }

    // ---- fairness: reads that completed at once, parked until the next round.
    private var round = 1L                                // servedRound starts at 0: never "this round"
    // As in the readiness reactors: the lists hold fds, the parked reader is per fd, so closing the stream can fail
    // its deferred reader (ClosedException) and the next round then finds nothing to resume for that fd.
    private var deferred = ArrayList<Int>()
    private var deferredNext = ArrayList<Int>()

    private suspend fun waitNextRound(fd: Int) {
        suspendCoroutineUninterceptedOrReturn<Unit> { cont ->
            ix(fd).let { deferredReader[it] = cont }
            deferred.add(fd)
            COROUTINE_SUSPENDED
        }
        kotlin.coroutines.coroutineContext[Job]?.let { if (!it.isActive) throw it.getCancellationException() }
    }

    // ---- reads and writes (SPEC §24, as in UringReactor): the reactor completes them from the
    // completion packet and resumes the caller; the fast paths are tail calls, nothing is allocated.

    override suspend fun read(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        checkOwner("read")
        val i = ix(fd)
        if (readBusy[i] || readDeferred[i]) throw IllegalStateException("concurrent read on socket $fd")
        if (servedRound[i] == round) return readNextRound(fd, dst, sizer)
        return readNow(fd, dst, sizer)
    }

    private suspend fun readNextRound(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        val i = ix(fd)
        readDeferred[i] = true
        try { waitNextRound(fd) } finally { readDeferred[i] = false }
        return readNow(fd, dst, sizer)
    }

    private suspend fun readNow(fd: Int, dst: Buffer, sizer: ReadSizer): Int {
        val skip = if (ensureAssociated(fd)) 1 else 0
        val cap = dst.reserve(sizer.readChunk(), bufferPool)     // may replace the backing array: pin after
        val pin = pinFor(fd, dst.backingArray())
        val at = dst.writerIndex()
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.kind = KIND_RECV; slot.cancelOnAbort = true; slot.isWrite = false
        slot.pin = pin; pin.refs++
        slot.readBuf = dst; slot.readSizer = sizer
        ix(fd).let { readBusy[it] = true }
        return suspendCoroutineUninterceptedOrReturn { cont ->
            try { watchCancellation(fd, cont) } catch (t: Throwable) { releaseSlot(idx, slot); throw t }
            when (val rc = neton_recv(fd.toSocket(), slot.op, pin.pinned.addressOf(at), cap.toUInt(), skip)) {
                0 -> {                              // completed at once: no packet follows
                    val n = neton_op_bytes(slot.op).toInt()
                    releaseSlot(idx, slot)
                    boxedInt(commitRead(fd, dst, sizer, n, sync = true))
                }
                1 -> { slot.cont = cont; COROUTINE_SUSPENDED }
                else -> { releaseSlot(idx, slot); throw IoException("WSARecv failed: ${errnoMessage(rc)}", rc) }
            }
        }
    }

    /** Commit [n] received bytes into [dst]; -1 at EOF. A read that completed at once is this fd's turn (§19.5). */
    private fun commitRead(fd: Int, dst: Buffer, sizer: ReadSizer, n: Int, sync: Boolean): Int {
        if (n <= 0) return -1                        // 0 bytes: orderly shutdown by the peer
        dst.commitWrite(n)
        sizer.onRead(n)
        stats?.let { it.reads++; it.readBytes += n }
        if (sync) ix(fd).let { servedRound[it] = round }
        return n
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        checkOwner("write")
        val w = ix(fd)
        if (writeBusy[w]) throw IllegalStateException("concurrent write on socket $fd")
        if (src.readableBytes == 0) return 0
        writeBusy[w] = true
        try {
            var total = 0
            while (src.readableBytes > 0) {
                if (staged[w] != 0) { awaitStaged(fd, w); continue }
                val n = sendPinned(fd, pinForWrite(fd, src.backingArray()).pinned, src.readerIndex(), src.readableBytes)
                when {
                    n > 0 -> { src.consumeSent(n); total += n; stats?.let { it.writes++; it.writeBytes += n } }
                    n == WOULD_BLOCK -> {
                        val len = minOf(src.readableBytes, MAX_SEND_CHUNK)
                        stageSend(fd, w, len) { dst ->
                            src.backingArray().usePinned { p -> memcpy(dst, p.addressOf(src.readerIndex()), len.convert()) }
                            src.consumeSent(len)
                        }
                        total += len
                    }
                    else -> { val e = lastSocketError(); throw IoException("send failed: ${errnoMessage(e)}", e) }
                }
            }
            return total
        } finally {
            writeBusy[w] = false
        }
    }

    override suspend fun writev(fd: Int, bufs: Array<Buffer>, count: Int): Long {
        checkOwner("writev")
        val w = ix(fd)
        if (writeBusy[w]) throw IllegalStateException("concurrent write on socket $fd")
        writeBusy[w] = true
        try {
            var total = 0L
            var i = 0
            while (i < count && bufs[i].readableBytes == 0) i++
            while (i < count) {
                if (staged[w] != 0) { awaitStaged(fd, w); continue }
                val n = minOf(MAX_IOV, count - i)
                val pins = arrayOfNulls<Pinned<ByteArray>>(n)
                for (k in 0 until n) pins[k] = bufs[i + k].backingArray().pin()
                val sent = try { sendBuffers(fd, bufs, i, n, pins) } finally { for (p in pins) p?.unpin() }
                when {
                    sent > 0 -> { total += sent; stats?.let { it.writes++; it.writeBytes += sent }; i = advanceBuffers(bufs, i, count, sent) }
                    sent == WOULD_BLOCK.toLong() -> {
                        var len = 0
                        var k = i
                        while (k < count && len < MAX_SEND_CHUNK) { len += minOf(bufs[k].readableBytes, MAX_SEND_CHUNK - len); k++ }
                        stageSend(fd, w, len) { dst ->
                            var off = 0
                            var j = i
                            while (off < len) {
                                val b = bufs[j++]
                                val take = minOf(b.readableBytes, len - off)
                                if (take > 0) b.backingArray().usePinned { p -> memcpy(dst + off, p.addressOf(b.readerIndex()), take.convert()) }
                                off += take
                            }
                            i = advanceBuffers(bufs, i, count, len.toLong())
                        }
                        total += len
                    }
                    else -> { val e = lastSocketError(); throw IoException("send failed: ${errnoMessage(e)}", e) }
                }
            }
            return total
        } finally {
            writeBusy[w] = false
        }
    }

    /**
     * A send that would block (SPEC §33.3): [fill] copies its [len] bytes into a block the reactor owns and consumes
     * them from the caller's buffers, so they count as sent once handed to the kernel; an overlapped WSASend then sends
     * the copy, and the writer waits for it. Windows cannot cancel a pending TCP send exactly (a cancelled WSASend
     * reported 0 bytes while the kernel sent all of them, and a successor sent them again), so this send is never
     * cancelled for its writer: a cancelled or timed-out writer resumes at once and the send finishes on its own, or
     * with the socket. One per socket: the next write waits for it, which keeps the order and bounds the memory.
     */
    private suspend inline fun stageSend(fd: Int, w: Int, len: Int, fill: (CPointer<ByteVar>) -> Unit) {
        val block = malloc(len.convert())?.reinterpret<ByteVar>() ?: throw IoException("out of memory for a $len-byte send", 0)
        try { fill(block) } catch (t: Throwable) { free(block); throw t }
        val skip = if (ensureAssociated(fd)) 1 else 0
        val idx = takeSlot()
        val slot = slots[idx]!!
        slot.fd = fd; slot.kind = KIND_SEND; slot.cancelOnAbort = false; slot.isWrite = true
        slot.nativeBlock = block; slot.stage = len; slot.stageOff = 0
        stats?.let { it.writes++; it.writeBytes += len }
        val rc = startStaged(fd, slot, skip)
        if (rc == 0) { releaseSlot(idx, slot); return }       // the kernel took it at once
        if (rc != 1) { releaseSlot(idx, slot); throw IoException("WSASend failed: ${errnoMessage(rc)}", rc) }
        staged[w] = idx + 1
        awaitStaged(fd, w)
    }

    /** Send the rest of a staged copy: 0 when the kernel took all of it, 1 when a send is pending, else the error. */
    private fun startStaged(fd: Int, slot: Slot, skip: Int): Int {
        while (true) {
            val rc = neton_send(fd.toSocket(), slot.op, slot.nativeBlock!!.reinterpret<ByteVar>() + slot.stageOff,
                (slot.stage - slot.stageOff).toUInt(), skip)
            if (rc != 0) return rc
            slot.stageOff += neton_op_bytes(slot.op).toInt()
            if (slot.stageOff >= slot.stage) return 0
        }
    }

    /** Wait until the socket's staged send has been taken by the kernel; a cancel or timeout only ends the wait. */
    private suspend fun awaitStaged(fd: Int, w: Int) {
        val slot = slots[staged[w] - 1]!!
        suspendCoroutineUninterceptedOrReturn<Int> { cont ->
            watchCancellation(fd, cont)
            slot.cont = cont
            COROUTINE_SUSPENDED
        }
    }

    /**
     * A read whose caller gave up (cancelled, timed out): cancel the I/O and resume the caller
     * with [cause] from the completion, once the bytes that did move are accounted in its buffer.
     */
    private fun abortOp(slot: Slot, cause: Throwable) {
        if (slot.abort != null) return
        slot.abort = cause
        neton_cancel(slot.fd.toSocket(), slot.op)
    }

    // SPEC §29.7: UDP readiness on the port. The datagram I/O itself is the non-blocking WSARecvMsg / WSASendMsg of
    // Udp.mingw.kt, as on the readiness drivers. Readable: a zero-byte overlapped WSARecv with MSG_PEEK completes
    // when a datagram is queued and takes nothing (libuv's zero read for UDP).
    override suspend fun awaitReadable(fd: Int) {
        checkOwner("recv")
        submit(fd, KIND_PEEK, null, cancelOnAbort = true, isWrite = false) { op, skip -> neton_udp_peek(fd.toSocket(), op, skip) }
    }

    // Writable: a UDP send waits only while the send buffer is full, rarely; a WSAPoll check on a short backoff, as
    // awaitConnect.
    override suspend fun awaitWritable(fd: Int) {
        checkOwner("send")
        var backoff = 1L
        while (neton_poll_writable(fd.toSocket()) == 0) { kotlinx.coroutines.delay(backoff); backoff = minOf(backoff * 2, 10L) }
    }

    override suspend fun accept(listenFd: Int): Int {
        checkOwner("accept")
        while (true) {
            // Fast path: a connection already queued is taken without an overlapped op.
            acceptNow(listenFd)?.let { return it }
            if (ix(listenFd).let { pollAccept[it] }) return pollingAccept(listenFd)
            try {
                return submit(listenFd, KIND_ACCEPT, null, cancelOnAbort = true, isWrite = false) { op, skip ->
                    neton_accept(listenFd.toSocket(), op, skip)
                }.also { setNonBlocking(it) }
            } catch (_: PendingConnectionGone) {
                continue                                      // that client reset before it was accepted: take the next
            } catch (e: IoException) {
                // AcceptEx is not offered for every address family (AF_UNIX): fall back to polling accept().
                if (e.errno != WSAEOPNOTSUPP && e.errno != WSAEINVAL) throw e
                ix(listenFd).let { pollAccept[it] = true }
                return pollingAccept(listenFd)
            }
        }
    }

    /** An AcceptEx whose connection was reset or aborted before it completed: the listener is fine, accept again. */
    private class PendingConnectionGone : Exception()

    /** Non-blocking accept(): the new fd, null if none is queued; throws if the listener is gone or failed. */
    private fun acceptNow(listenFd: Int): Int? {
        val fd = acceptOne(listenFd)
        if (fd >= 0) { setNonBlocking(fd); return fd }
        val e = lastSocketError()
        // A queued connection the client reset before it was taken: none for now, the listener is fine.
        if (e == WSAEWOULDBLOCK_CODE || e == WSAEINTR_CODE || e == WSAECONNRESET_CODE) return null
        if (e == WSAENOTSOCK || e == WSAEINVAL) throw ClosedException("listener closed")
        throw IoException("accept failed: ${errnoMessage(e)}", e)
    }

    private suspend fun pollingAccept(listenFd: Int): Int {
        var backoff = 1L
        while (true) {
            acceptNow(listenFd)?.let { return it }
            kotlinx.coroutines.delay(backoff); backoff = minOf(backoff * 2, 10L)
        }
    }

    override suspend fun awaitConnect(fd: Int) {
        var backoff = 1L
        while (neton_connect_done(fd.toSocket()) == 0) { kotlinx.coroutines.delay(backoff); backoff = minOf(backoff * 2, 10L) }
    }

    override fun closeStream(fd: Int) {
        checkOwner("close")
        var any = false
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (!slot.live || slot.fd != fd) continue
            any = true
            // SPEC §28.6: the op keeps its awaiter until its completion packet says the kernel is done
            // with the buffer; a read or send ends with ClosedException (or the cause it already had),
            // other ops with ClosedException from the aborted completion.
            // Every op it closes ends with ClosedException, whatever error its packet carries: closesocket can win
            // the race with the cancel, and the packet then says WSAENOTSOCK (a UDP peek, CI).
            if (slot.cont != null && slot.abort == null) slot.abort = ClosedException()
        }
        if (any) neton_cancel(fd.toSocket(), null)   // their packets arrive (aborted) and free the slots
        forgetWatches(fd)
        retirePin(fd)
        val i = ix(fd)
        staged[i] = 0                                     // its slot ends with the aborted packet
        // A reader deferred to the next round must not recv on this fd number (the kernel may reuse it): fail it now.
        deferredReader[i]?.let { deferredReader[i] = null; enqueueResume(it, ClosedException()) }
        associated[i] = false; skipOn[i] = false; servedRound[i] = -1; pollAccept[i] = false
        closeFd(fd)
    }

    // ---- wakeups: at most one packet in flight; the loop clears the flag before absorbing.
    private val wakePending = AtomicInt(0)

    override fun wakeup() {
        if (wakePending.compareAndSet(0, 1)) neton_iocp_post(port)
    }

    private val batch = 256
    private val ops = nativeHeap.allocArray<COpaquePointerVar>(batch)
    private val bytes = nativeHeap.allocArray<UIntVar>(batch)
    private val errs = nativeHeap.allocArray<IntVar>(batch)

    override fun runUntil(root: Job) {
        while (true) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (readyToStop(root)) break
            val timerMs = nextTimerMillis()
            val block = !(hasTasks() || deferredNext.isNotEmpty() || deferred.isNotEmpty() || timerMs == 0)
            val timeout: UInt = if (!block) 0u else if (timerMs < 0) INFINITE else timerMs.toUInt()
            val n = neton_iocp_wait(port, ops, bytes, errs, batch, timeout)
            countPoll(!block, n)
            if (n < 0) throw IoException("GetQueuedCompletionStatusEx failed: Win32 error ${-n}", -n)
            for (k in 0 until n) onCompletion(ops[k], bytes[k].toInt(), errs[k])
            round++
            // Reads deferred for fairness resume in the next round, after this round's completions.
            val d = deferred; deferred = deferredNext; deferredNext = d
            for (fd in deferredNext) {
                val i = ix(fd)
                val c = deferredReader[i] ?: continue             // its stream was closed meanwhile: already failed
                deferredReader[i] = null
                enqueueResume(c)
            }
            deferredNext.clear()
        }
    }

    private fun onCompletion(op: COpaquePointer?, n: Int, err: Int) {
        if (op == null) { wakePending.store(0); absorbExternal(); return }
        val idx = neton_op_slot(op).toInt()
        val slot = slots.getOrNull(idx) ?: return
        if (!slot.live || slot.gen != neton_op_gen(op)) return
        if (slot.readBuf != null) { onReadDone(idx, slot, n, err); return }
        if (slot.stage > 0) { onStagedDone(idx, slot, n, err); return }
        val fd = slot.fd
        val cont = slot.cont
        var value = n
        var failure: Throwable? = null
        if (err == ERROR_OPERATION_ABORTED && n > 0 && slot.kind != KIND_ACCEPT) {
            // Cancelled (e.g. the stream was closed) after bytes moved: they count (SPEC §28.6).
        } else if (err != 0) {
            if (slot.kind == KIND_ACCEPT) neton_accept_discard(slot.op)
            failure = when {
                slot.abort != null -> slot.abort
                err == ERROR_OPERATION_ABORTED -> ClosedException()
                // A zero-byte peek: the datagram does not fit zero bytes, or ICMP reported a reset; either way a
                // receive will not block now (it reports the reset itself).
                slot.kind == KIND_PEEK && (err == WSAEMSGSIZE_CODE || err == WSAECONNRESET_CODE || err == WSAENETRESET_CODE) -> null
                // The listener was closed under the pending AcceptEx (shutdown): as acceptNow reports it.
                slot.kind == KIND_ACCEPT && (err == WSAENOTSOCK || err == WSAEINVAL) -> ClosedException("listener closed")
                // Only that connection is gone (reset before the accept completed).
                slot.kind == KIND_ACCEPT && (err == ERROR_NETNAME_DELETED || err == ERROR_CONNECTION_ABORTED ||
                    err == WSAECONNRESET_CODE || err == WSAECONNABORTED_CODE) -> PendingConnectionGone()
                else -> IoException("${KIND_NAMES[slot.kind]} failed: ${errnoMessage(err)}", err)
            }
        } else if (slot.kind == KIND_ACCEPT) {
            if (cont == null) neton_accept_discard(slot.op)       // nobody wants it any more
            else try { value = finishAccept(fd, slot) } catch (t: Throwable) { failure = t }
        }
        releaseSlot(idx, slot)
        if (cont == null) return
        if (failure != null) enqueueResumeInt(cont, 0, failure) else enqueueResumeInt(cont, value)
    }

    /** A read's completion: commit what arrived (also for an aborted read), then resume the reader. */
    private fun onReadDone(idx: Int, slot: Slot, n: Int, err: Int) {
        val cont = slot.cont
        val fd = slot.fd; val dst = slot.readBuf!!; val sizer = slot.readSizer!!; val abort = slot.abort
        releaseSlot(idx, slot)
        if (cont == null) return                              // stream closed: buffer abandoned
        val moved = if (n > 0 && (err == 0 || err == ERROR_OPERATION_ABORTED)) commitRead(fd, dst, sizer, n, sync = false) else 0
        when {
            abort != null -> enqueueResumeInt(cont, 0, abort)
            err == ERROR_OPERATION_ABORTED -> enqueueResumeInt(cont, 0, ClosedException())
            err != 0 -> enqueueResumeInt(cont, 0, IoException("WSARecv failed: ${errnoMessage(err)}", err))
            else -> enqueueResumeInt(cont, if (moved > 0) moved else -1)
        }
    }

    /** A staged send's completion: send what the kernel did not take yet, else end it and resume its waiter. */
    private fun onStagedDone(idx: Int, slot: Slot, n: Int, err: Int) {
        var e = err
        if (e == 0) {
            slot.stageOff += n
            if (slot.stageOff < slot.stage) {                // a partial send (not expected on TCP): the rest
                val skip = if (ix(slot.fd).let { skipOn[it] }) 1 else 0
                e = startStaged(slot.fd, slot, skip)
                if (e == 1) return                            // the next completion continues
            }
        }
        val w = ix(slot.fd)
        if (staged[w] == idx + 1) staged[w] = 0
        val cont = slot.cont
        val closed = slot.abort
        releaseSlot(idx, slot)
        if (cont == null) return                              // its writer gave up, or the socket closed
        if (e == 0) enqueueResumeInt(cont, 0)
        else enqueueResumeInt(cont, 0, closed ?: if (e == ERROR_OPERATION_ABORTED) ClosedException() else IoException("WSASend failed: ${errnoMessage(e)}", e))
    }

    /**
     * Cancel what is still in flight and wait (bounded) for the packets, so no buffer is released
     * while Windows may still touch it; if some never arrive their buffers stay pinned (a leak,
     * reported), never freed under the kernel.
     */
    // SPEC §28.3: the loop does not leave CLOSING-bound while the kernel still owns buffers.
    override fun inFlightKernelOps(): Int = liveOps

    override fun cancelInFlightOps() {
        for (i in slots.indices) {
            val slot = slots[i] ?: continue
            if (slot.live) { slot.cont = null; neton_cancel(slot.fd.toSocket(), slot.op) }
        }
    }

    override fun shutdown() {
        if (liveOps > 0) {
            for (i in slots.indices) {
                val slot = slots[i] ?: continue
                if (slot.live) { slot.cont = null; neton_cancel(slot.fd.toSocket(), slot.op) }
            }
            var rounds = 0
            while (liveOps > 0 && rounds < DRAIN_ROUNDS) {
                val n = neton_iocp_wait(port, ops, bytes, errs, batch, 10u)
                for (k in 0 until maxOf(n, 0)) onCompletion(ops[k], bytes[k].toInt(), errs[k])
                rounds++
            }
            if (liveOps > 0) fprintf(stderr, "neton-io: %d IOCP op(s) did not complete after cancel; buffers left pinned\n", liveOps)
        }
        for (s in slots) if (s != null && !s.live) neton_op_free(s.op)
        // Before the port closes: no cross-thread wakeup may post to a closed (or reused) handle (SPEC §27.12).
        closeWakePipe()
        neton_iocp_close(port)
        nativeHeap.free(ops.rawValue); nativeHeap.free(bytes.rawValue); nativeHeap.free(errs.rawValue)
    }

    private companion object {
        const val KIND_RECV = 1
        const val KIND_SEND = 2
        const val KIND_ACCEPT = 3
        const val KIND_PEEK = 4
        val KIND_NAMES = arrayOf("", "WSARecv", "WSASend", "AcceptEx", "WSARecv (peek)")
        const val INFINITE: UInt = 0xFFFF_FFFFu
        const val DRAIN_ROUNDS = 300
        const val ERROR_OPERATION_ABORTED = 995
        const val WSAEWOULDBLOCK_CODE = 10035
        const val WSAEINTR_CODE = 10004
        const val WSAENOTSOCK = 10038
        const val WSAEOPNOTSUPP = 10045
        const val WSAEINVAL = 10022
        const val WSAECONNRESET_CODE = 10054
        const val WSAEMSGSIZE_CODE = 10040
        const val WSAENETRESET_CODE = 10052
        const val WSAECONNABORTED_CODE = 10053
        const val ERROR_NETNAME_DELETED = 64
        const val ERROR_CONNECTION_ABORTED = 1236
    }
}
