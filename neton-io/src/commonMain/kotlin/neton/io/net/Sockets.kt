package neton.io.net

// Socket primitives, one implementation per platform family (SPEC §20): POSIX sockets in posixMain
// (Linux, Android, Apple), Winsock in mingwMain. `fd` is the platform's socket handle as an Int.

internal const val EOF_RESULT = 0
internal const val WOULD_BLOCK = -1
internal const val IO_ERROR = -2

/** Put [fd] into non-blocking mode. */
internal expect fun setNonBlocking(fd: Int)

/** Accept one connection; returns the client fd, or -1 when nothing is pending. */
internal expect fun acceptOne(listenFd: Int): Int

internal expect fun closeFd(fd: Int)

/** Pending socket error (SO_ERROR), e.g. the outcome of a non-blocking connect; 0 when none. */
internal expect fun socketError(fd: Int): Int

/** The error code of the last failed socket call on this thread (errno / WSAGetLastError). */
internal expect fun lastSocketError(): Int

internal expect fun errnoMessage(code: Int): String

/**
 * Writing to a peer that has gone away must surface as an error, never as SIGPIPE killing the
 * process. Done per socket / per call, never by changing the process signal disposition (a library
 * must not alter the host's signal handling): SO_NOSIGPIPE on Apple, MSG_NOSIGNAL on every send on
 * Linux/Android (and IORING_OP_SEND with MSG_NOSIGNAL on io_uring); Windows has no SIGPIPE.
 */
internal expect fun suppressSigpipe(fd: Int): Boolean

/** shutdown(SHUT_WR) / shutdown(SD_SEND). */
internal expect fun shutdownWrite(fd: Int)

/** Upper bound on buffers per vectored send (IOV_MAX is 1024 on Linux/Apple; stay well below). */
internal const val MAX_IOV = 64

/** Advance `bufs[from..]` by [n] sent bytes, in order; returns the index of the first buffer with bytes left. */
internal fun advanceBuffers(bufs: Array<neton.io.bytes.Buffer>, from: Int, end: Int, n: Long): Int {
    var left = n
    var i = from
    while (i < end && left > 0) {
        val b = bufs[i]
        val take = minOf(left, b.readableBytes.toLong()).toInt()
        b.consumeSent(take); left -= take
        if (b.readableBytes == 0) i++
    }
    while (i < end && bufs[i].readableBytes == 0) i++
    return i
}

/** A non-blocking [read end, write end] pair used to wake the reactor from another thread. */
internal expect fun createWakePipe(): IntArray

/** Write one byte; a full pipe already guarantees a wakeup, so would-block is fine. */
internal expect fun signalWakePipe(writeFd: Int)

/** Drain the pipe so the next signal is a fresh edge. */
internal expect fun drainWakePipe(readFd: Int)
