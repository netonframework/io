@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
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
import platform.posix.memset
import platform.posix.msghdr
import platform.posix.sendmsg
import platform.posix.shutdown
import kotlinx.cinterop.sizeOf

internal actual fun sendBuffers(fd: Int, bufs: Array<Buffer>, from: Int, count: Int): Long = memScoped {
    // Pinning keeps each array in place for the duration of the call only (the syscall is synchronous).
    val pins = Array(count) { bufs[from + it].backingArray().pin() }
    try {
        val iov = allocArray<iovec>(count)
        for (i in 0 until count) {
            val b = bufs[from + i]
            iov[i].iov_base = if (b.readableBytes == 0) null else pins[i].addressOf(b.readerIndex())   // an empty pooled buffer holds a 0-length array
            iov[i].iov_len = b.readableBytes.convert()
        }
        val msg = alloc<msghdr>()
        memset(msg.ptr, 0, sizeOf<msghdr>().convert())
        msg.msg_iov = iov
        msg.msg_iovlen = count.convert()
        val n = sendmsg(fd, msg.ptr, SEND_FLAGS).toLong()
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
