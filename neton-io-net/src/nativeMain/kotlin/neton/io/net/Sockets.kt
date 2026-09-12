package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
import neton.io.bytes.Buffer
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
import platform.posix.F_GETFL
import platform.posix.F_SETFL
import platform.posix.O_NONBLOCK
import platform.posix.accept
import platform.posix.close
import platform.posix.errno
import platform.posix.fcntl
import platform.posix.recv
import platform.posix.send

/** Outcome of a non-blocking socket op. */
internal enum class IoResult { OK, WOULD_BLOCK, EOF, ERROR }

/** Open a listening TCP socket bound to [host]:[port] (platform-specific address setup). */
internal expect fun tcpListen(host: String, port: Int, backlog: Int = 1024): Int

/** Open a TCP socket connecting to [host]:[port] (non-blocking connect completes via the reactor). */
internal expect fun tcpConnect(host: String, port: Int): Int

@OptIn(ExperimentalForeignApi::class)
internal fun setNonBlocking(fd: Int) {
    val flags = fcntl(fd, F_GETFL, 0)
    fcntl(fd, F_SETFL, flags or O_NONBLOCK)
}

/** Accept one connection; returns the client fd, or -1 when nothing is pending. */
@OptIn(ExperimentalForeignApi::class)
internal fun acceptOne(listenFd: Int): Int = accept(listenFd, null, null)

@OptIn(ExperimentalForeignApi::class)
internal fun closeFd(fd: Int) {
    close(fd)
}

/** Number of bytes read into [buf] when [result] is OK. */
internal class ReadOutcome(val result: IoResult, val count: Int)

/**
 * Non-blocking read straight into [buf]'s backing memory — no intermediate array. Reserves
 * up to [chunk] writable bytes, receives into them, and commits the count.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun readInto(fd: Int, buf: Buffer, chunk: Int): ReadOutcome {
    val cap = buf.reserve(chunk)
    val n = buf.backingArray().usePinned { pinned ->
        recv(fd, pinned.addressOf(buf.writerIndex()), cap.convert(), 0).toInt()
    }
    return when {
        n > 0 -> {
            buf.commitWrite(n)
            ReadOutcome(IoResult.OK, n)
        }
        n == 0 -> ReadOutcome(IoResult.EOF, 0)
        errno == EINTR -> ReadOutcome(IoResult.WOULD_BLOCK, 0)
        errno == EAGAIN || errno == EWOULDBLOCK -> ReadOutcome(IoResult.WOULD_BLOCK, 0)
        else -> ReadOutcome(IoResult.ERROR, 0)
    }
}

/**
 * Non-blocking write straight from [buf]'s readable region — no intermediate array. Sends
 * as much as the kernel accepts and consumes it. Returns bytes written, or [WOULD_BLOCK]/[IO_ERROR].
 */
@OptIn(ExperimentalForeignApi::class)
internal fun writeFrom(fd: Int, buf: Buffer): Int {
    val len = buf.readableBytes
    if (len == 0) return 0
    val n = buf.backingArray().usePinned { pinned ->
        send(fd, pinned.addressOf(buf.readerIndex()), len.convert(), 0).toInt()
    }
    if (n > 0) {
        buf.consume(n)
        return n
    }
    return if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) WOULD_BLOCK else IO_ERROR
}

internal const val WOULD_BLOCK = -1
internal const val IO_ERROR = -2
