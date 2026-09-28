@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.COpaquePointerVar
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.set
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.pin
import kotlinx.cinterop.ptr
import neton.io.bytes.Buffer
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
import platform.posix.SHUT_WR
import platform.posix.errno
import platform.posix.iovec
import platform.posix.shutdown
import kotlinx.cinterop.sizeOf

/** The iovec parts of a vectored send, allocated once per thread (sends are synchronous and never nested). */
@kotlin.native.concurrent.ThreadLocal
private object VectoredScratch {
    val bases = nativeHeap.allocArray<COpaquePointerVar>(MAX_IOV)
    val lens = nativeHeap.allocArray<IntVar>(MAX_IOV)
}

/**
 * One vectored send of `bufs[from until from + count]` (at most [MAX_IOV]); [pins] holds each buffer's array pinned at
 * the same position (the reactor caches them). The iovec array is built in C (neton_sendv): its field widths differ
 * across the POSIX targets.
 */
internal actual fun sendBuffers(fd: Int, bufs: Array<Buffer>, from: Int, count: Int, pins: Array<Pinned<ByteArray>?>): Long {
    val bases = VectoredScratch.bases
    val lens = VectoredScratch.lens
    for (i in 0 until count) {
        val b = bufs[from + i]
        bases[i] = if (b.readableBytes == 0) null else pins[i]!!.addressOf(b.readerIndex())   // an empty pooled buffer holds a 0-length array
        lens[i] = b.readableBytes
    }
    val n = neton.io.posixshim.neton_sendv(fd, bases, lens, count, SEND_FLAGS)
    return when {
        n >= 0 -> n
        errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR -> WOULD_BLOCK.toLong()
        else -> IO_ERROR.toLong()
    }
}

internal actual fun shutdownWrite(fd: Int) {
    shutdown(fd, SHUT_WR)
}
