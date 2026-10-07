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
import kotlinx.cinterop.Pinned
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

internal actual fun sendBuffers(fd: Int, bufs: Array<Buffer>, from: Int, count: Int, pins: Array<Pinned<ByteArray>?>): Long = memScoped {
    run {
        val wsabufs = allocArray<WSABUF>(count)
        // At most MAX_SEND_CHUNK in one call, as in sendPinned: Windows would otherwise take every buffer at once.
        var budget = MAX_SEND_CHUNK
        var used = 0
        for (i in 0 until count) {
            val b = bufs[from + i]
            val len = minOf(b.readableBytes, budget)
            wsabufs[i].len = len.convert()
            wsabufs[i].buf = if (len == 0) null else pins[i]!!.addressOf(b.readerIndex())   // an empty pooled buffer holds a 0-length array
            used = i + 1
            budget -= len
            if (budget == 0) break
        }
        val sent = alloc<UIntVar>()
        val rc = WSASend(fd.toSocket(), wsabufs, used.convert(), sent.ptr, 0u, null, null)
        if (rc == SOCKET_ERROR) failedIo().toLong() else sent.value.toLong()
    }
}

internal actual fun shutdownWrite(fd: Int) {
    shutdown(fd.toSocket(), SD_SEND)
}
