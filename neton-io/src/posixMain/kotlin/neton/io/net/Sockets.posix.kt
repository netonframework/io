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
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
import platform.posix.F_GETFL
import platform.posix.F_SETFL
import platform.posix.O_NONBLOCK
import platform.posix.SOL_SOCKET
import platform.posix.SO_ERROR
import platform.posix.close
import platform.posix.errno
import platform.posix.fcntl
import platform.posix.pipe
import platform.posix.strerror

@OptIn(ExperimentalForeignApi::class)
internal actual fun setNonBlocking(fd: Int) {
    val flags = fcntl(fd, F_GETFL, 0)
    fcntl(fd, F_SETFL, flags or O_NONBLOCK)
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun acceptOne(listenFd: Int): Int = neton.io.posixshim.neton_accept(listenFd)

@OptIn(ExperimentalForeignApi::class)
internal actual fun closeFd(fd: Int) {
    close(fd)
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun socketError(fd: Int): Int = memScoped {
    val err = alloc<IntVar>()
    if (neton.io.posixshim.neton_getsockopt_int(fd, SOL_SOCKET, SO_ERROR, err.ptr) != 0) errno else err.value
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun lastSocketError(): Int = errno

@OptIn(ExperimentalForeignApi::class)
internal actual fun errnoMessage(code: Int): String = strerror(code)?.toKString() ?: "errno $code"

/** Flags for send(2) on this platform (MSG_NOSIGNAL on Linux, nothing on Apple). */
internal expect val SEND_FLAGS: Int   // actual in epollMain (MSG_NOSIGNAL) and appleMain (0)

@OptIn(ExperimentalForeignApi::class)
internal actual fun recvPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = neton.io.posixshim.neton_recv(fd, pinned.addressOf(offset), len)
    return when {
        n > 0 -> n
        n == 0 -> EOF_RESULT
        errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR -> WOULD_BLOCK
        else -> IO_ERROR
    }
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun sendPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = neton.io.posixshim.neton_send(fd, pinned.addressOf(offset), len, SEND_FLAGS)
    if (n > 0) return n
    return if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) WOULD_BLOCK else IO_ERROR
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun createWakePipe(): IntArray = memScoped {
    val fds = allocArray<IntVar>(2)
    check(pipe(fds) == 0) { "pipe() failed: ${errnoMessage(errno)}" }
    setNonBlocking(fds[0]); setNonBlocking(fds[1])
    pipeNoSigpipe(fds[1])
    intArrayOf(fds[0], fds[1])
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun signalWakePipe(writeFd: Int) = neton.io.posixshim.neton_write_byte(writeFd)

@OptIn(ExperimentalForeignApi::class)
internal actual fun drainWakePipe(readFd: Int) = neton.io.posixshim.neton_drain(readFd)

/** A write to [fd] (a pipe) must not raise SIGPIPE: Apple has F_SETNOSIGPIPE; elsewhere a no-op. */
internal expect fun pipeNoSigpipe(fd: Int)

