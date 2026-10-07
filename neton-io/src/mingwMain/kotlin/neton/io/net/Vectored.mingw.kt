@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.Pinned
import neton.io.bytes.Buffer
import neton.io.win.neton_sendv_nb
import platform.posix.WSABUF
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
        val rc = neton_sendv_nb(fd.toSocket(), wsabufs, used.convert())
        if (rc < 0) failedIo((-rc).toInt()).toLong() else rc
    }
}

internal actual fun shutdownWrite(fd: Int) {
    shutdown(fd.toSocket(), SD_SEND)
}
