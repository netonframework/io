package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.get
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.toKString
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.value
import neton.io.bytes.Buffer
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
import platform.posix.F_GETFL
import platform.posix.F_SETFL
import platform.posix.O_NONBLOCK
import platform.posix.SOL_SOCKET
import platform.posix.SO_ERROR
import platform.posix.accept
import platform.posix.close
import platform.posix.errno
import platform.posix.fcntl
import platform.posix.getsockopt
import platform.posix.pipe
import platform.posix.read
import platform.posix.write
import platform.posix.recv
import platform.posix.send
import platform.posix.socklen_tVar
import platform.posix.strerror

/** Outcome of a non-blocking socket op. */
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

/** Pending socket error (SO_ERROR), e.g. the outcome of a non-blocking connect; 0 when none. */
@OptIn(ExperimentalForeignApi::class)
internal fun socketError(fd: Int): Int = memScoped {
    val err = alloc<IntVar>()
    val len = alloc<socklen_tVar>()
    len.value = sizeOf<IntVar>().convert()
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, err.ptr, len.ptr) != 0) errno else err.value
}

@OptIn(ExperimentalForeignApi::class)
internal fun errnoMessage(code: Int): String = strerror(code)?.toKString() ?: "errno $code"

/**
 * Writing to a peer that has gone away must surface as EPIPE/ECONNRESET, never as SIGPIPE killing
 * the process. This is done per socket / per call, not by changing the process signal disposition
 * (a library must not alter the host's signal handling): Apple sets SO_NOSIGPIPE on every socket
 * the reactor creates or accepts; Linux passes MSG_NOSIGNAL on every send (readiness path) and
 * uses IORING_OP_SEND with MSG_NOSIGNAL (io_uring path).
 */
internal expect fun suppressSigpipe(fd: Int)

/** Flags for send(2) on this platform (MSG_NOSIGNAL on Linux, nothing on Apple). */
internal expect val SEND_FLAGS: Int

/**
 * Non-blocking recv into [pinned] at [offset], at most [len] bytes. Returns the byte count (>0),
 * [EOF_RESULT] on a clean peer close, [WOULD_BLOCK] (EAGAIN/EINTR) or [IO_ERROR] (errno is kept).
 * The caller commits the count into its [Buffer]; nothing is allocated here (SPEC §17c).
 */
@OptIn(ExperimentalForeignApi::class)
internal fun recvPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = recv(fd, pinned.addressOf(offset), len.convert(), 0).toInt()
    return when {
        n > 0 -> n
        n == 0 -> EOF_RESULT
        errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR -> WOULD_BLOCK
        else -> IO_ERROR
    }
}

/** Non-blocking send of [len] bytes from [pinned] at [offset]. Returns bytes sent, or [WOULD_BLOCK]/[IO_ERROR]. */
@OptIn(ExperimentalForeignApi::class)
internal fun sendPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = send(fd, pinned.addressOf(offset), len.convert(), SEND_FLAGS).toInt()
    if (n > 0) return n
    return if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) WOULD_BLOCK else IO_ERROR
}

internal const val EOF_RESULT = 0
internal const val WOULD_BLOCK = -1
internal const val IO_ERROR = -2

/** A non-blocking self-pipe [read fd, write fd] used to wake the reactor from another thread. */
@OptIn(ExperimentalForeignApi::class)
internal fun createWakePipe(): IntArray = memScoped {
    val fds = allocArray<IntVar>(2)
    check(pipe(fds) == 0) { "pipe() failed: ${errnoMessage(errno)}" }
    setNonBlocking(fds[0]); setNonBlocking(fds[1])
    intArrayOf(fds[0], fds[1])
}

/** Write one byte; a full pipe already guarantees a wakeup, so EAGAIN is fine. */
@OptIn(ExperimentalForeignApi::class)
internal fun signalWakePipe(writeFd: Int) = memScoped {
    val b = alloc<kotlinx.cinterop.ByteVar>()
    b.value = 1
    write(writeFd, b.ptr, 1u)
    Unit
}

/** Drain the pipe so the next signal is a fresh edge. */
@OptIn(ExperimentalForeignApi::class)
internal fun drainWakePipe(readFd: Int) = memScoped {
    val buf = allocArray<kotlinx.cinterop.ByteVar>(64)
    while (read(readFd, buf, 64u) > 0) { /* drain */ }
}
