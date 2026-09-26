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

@OptIn(ExperimentalForeignApi::class)
internal actual fun setNonBlocking(fd: Int) {
    val flags = fcntl(fd, F_GETFL, 0)
    fcntl(fd, F_SETFL, flags or O_NONBLOCK)
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun acceptOne(listenFd: Int): Int = accept(listenFd, null, null)

@OptIn(ExperimentalForeignApi::class)
internal actual fun closeFd(fd: Int) {
    close(fd)
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun socketError(fd: Int): Int = memScoped {
    val err = alloc<IntVar>()
    val len = alloc<socklen_tVar>()
    len.value = sizeOf<IntVar>().convert()
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, err.ptr, len.ptr) != 0) errno else err.value
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun lastSocketError(): Int = errno

@OptIn(ExperimentalForeignApi::class)
internal actual fun errnoMessage(code: Int): String = strerror(code)?.toKString() ?: "errno $code"

/** Flags for send(2) on this platform (MSG_NOSIGNAL on Linux, nothing on Apple). */
internal expect val SEND_FLAGS: Int   // actual in epollMain (MSG_NOSIGNAL) and appleMain (0)

@OptIn(ExperimentalForeignApi::class)
internal actual fun recvPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = recv(fd, pinned.addressOf(offset), len.convert(), 0).toInt()
    return when {
        n > 0 -> n
        n == 0 -> EOF_RESULT
        errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR -> WOULD_BLOCK
        else -> IO_ERROR
    }
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun sendPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = send(fd, pinned.addressOf(offset), len.convert(), SEND_FLAGS).toInt()
    if (n > 0) return n
    return if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) WOULD_BLOCK else IO_ERROR
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun createWakePipe(): IntArray = memScoped {
    val fds = allocArray<IntVar>(2)
    check(pipe(fds) == 0) { "pipe() failed: ${errnoMessage(errno)}" }
    setNonBlocking(fds[0]); setNonBlocking(fds[1])
    intArrayOf(fds[0], fds[1])
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun signalWakePipe(writeFd: Int): Unit = memScoped {
    val b = alloc<kotlinx.cinterop.ByteVar>()
    b.value = 1
    write(writeFd, b.ptr, 1u)
    Unit
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun drainWakePipe(readFd: Int): Unit = memScoped {
    val buf = allocArray<kotlinx.cinterop.ByteVar>(64)
    while (read(readFd, buf, 64u) > 0) { /* drain */ }
}
