@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pin
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import neton.io.bytes.Buffer
import platform.posix.SOCKET_ERROR
import platform.posix.WSABUF
import platform.posix.WSAEINTR
import platform.posix.WSAEWOULDBLOCK
import platform.posix.WSAGetLastError
import platform.posix.WSASend
import platform.posix.shutdown

/** SD_SEND: stop sending (winsock2.h). */
private const val SD_SEND = 1

internal actual fun sendBuffers(fd: Int, bufs: Array<Buffer>, from: Int, count: Int): Long = memScoped {
    val pins = Array(count) { bufs[from + it].backingArray().pin() }
    try {
        val wsabufs = allocArray<WSABUF>(count)
        for (i in 0 until count) {
            val b = bufs[from + i]
            wsabufs[i].len = b.readableBytes.convert()
            wsabufs[i].buf = pins[i].addressOf(b.readerIndex())
        }
        val sent = alloc<UIntVar>()
        val rc = WSASend(fd.toSocket(), wsabufs, count.convert(), sent.ptr, 0u, null, null)
        if (rc == SOCKET_ERROR) {
            val e = WSAGetLastError()
            if (e == WSAEWOULDBLOCK || e == WSAEINTR) WOULD_BLOCK.toLong() else IO_ERROR.toLong()
        } else sent.value.toLong()
    } finally {
        for (p in pins) p.unpin()
    }
}

internal actual fun shutdownWrite(fd: Int) {
    shutdown(fd.toSocket(), SD_SEND)
}
