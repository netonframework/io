package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
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

/**
 * Open a listening TCP socket bound to [host]:[port]. The address handling differs per
 * platform (sockaddr layout), so this is implemented in the apple/linux source sets.
 */
internal expect fun tcpListen(host: String, port: Int, backlog: Int = 128): Int

/** Open a TCP socket connected to [host]:[port] (non-blocking connect completes via the reactor). */
internal expect fun tcpConnect(host: String, port: Int): Int

@OptIn(ExperimentalForeignApi::class)
internal fun setNonBlocking(fd: Int) {
    val flags = fcntl(fd, F_GETFL, 0)
    fcntl(fd, F_SETFL, flags or O_NONBLOCK)
}

/** Accept one connection; returns the client fd, or -1 when there is nothing pending. */
@OptIn(ExperimentalForeignApi::class)
internal fun acceptOne(listenFd: Int): Int = accept(listenFd, null, null)

@OptIn(ExperimentalForeignApi::class)
internal fun closeFd(fd: Int) {
    close(fd)
}

/** Result of a read attempt: [count] valid only when [result] == OK. */
internal class ReadOutcome(val result: IoResult, val count: Int)

/** Non-blocking read of up to [buf].size bytes; never blocks the thread. */
@OptIn(ExperimentalForeignApi::class)
internal fun readOnce(fd: Int, buf: ByteArray): ReadOutcome {
    val n = buf.usePinned { pinned ->
        recv(fd, pinned.addressOf(0), buf.size.convert(), 0).toInt()
    }
    return when {
        n > 0 -> ReadOutcome(IoResult.OK, n)
        n == 0 -> ReadOutcome(IoResult.EOF, 0)
        errno == EINTR -> ReadOutcome(IoResult.WOULD_BLOCK, 0) // retry
        errno == EAGAIN || errno == EWOULDBLOCK -> ReadOutcome(IoResult.WOULD_BLOCK, 0)
        else -> ReadOutcome(IoResult.ERROR, 0)
    }
}

/** Non-blocking write of [bytes] starting at [offset]; returns bytes written (>=0) or -1 on error/would-block distinction via [wouldBlock]. */
@OptIn(ExperimentalForeignApi::class)
internal fun writeOnce(fd: Int, bytes: ByteArray, offset: Int): Int {
    val n = bytes.usePinned { pinned ->
        send(fd, pinned.addressOf(offset), (bytes.size - offset).convert(), 0).toInt()
    }
    if (n >= 0) return n
    return if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) WOULD_BLOCK else IO_ERROR
}

internal const val WOULD_BLOCK = -1
internal const val IO_ERROR = -2
