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
import kotlinx.cinterop.memScoped
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

internal actual fun sendBuffers(fd: Int, bufs: Array<Buffer>, from: Int, count: Int): Long = memScoped {
    // Pinning keeps each array in place for the duration of the call only (the syscall is synchronous).
    // The iovec array is built in C (neton_sendv): its field widths differ across the POSIX targets.
    val pins = Array(count) { bufs[from + it].backingArray().pin() }
    try {
        val bases = allocArray<COpaquePointerVar>(count)
        val lens = allocArray<IntVar>(count)
        for (i in 0 until count) {
            val b = bufs[from + i]
            bases[i] = if (b.readableBytes == 0) null else pins[i].addressOf(b.readerIndex())   // an empty pooled buffer holds a 0-length array
            lens[i] = b.readableBytes
        }
        val n = neton.io.posixshim.neton_sendv(fd, bases, lens, count, SEND_FLAGS)
        when {
            n >= 0 -> n
            errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR -> WOULD_BLOCK.toLong()
            else -> IO_ERROR.toLong()
        }
    } finally {
        for (p in pins) p.unpin()
    }
}

internal actual fun shutdownWrite(fd: Int) {
    shutdown(fd, SHUT_WR)
}
