@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class, kotlin.concurrent.atomics.ExperimentalAtomicApi::class)

package neton.io.core

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
import platform.posix.EINTR
import platform.posix.O_CLOEXEC
import platform.posix.O_RDONLY
import platform.posix.errno
import kotlin.concurrent.atomics.AtomicInt

/** /dev/urandom, opened once for the process (never closed; reads are thread-safe). */
private val urandom = AtomicInt(-1)

private fun urandomFd(): Int {
    val fd = urandom.load()
    if (fd >= 0) return fd
    val opened = platform.posix.open("/dev/urandom", O_RDONLY or O_CLOEXEC)
    check(opened >= 0) { "cannot open /dev/urandom (errno=$errno)" }
    if (!urandom.compareAndSet(-1, opened)) platform.posix.close(opened)
    return urandom.load()
}

internal actual fun platformSecureRandom(dst: ByteArray, offset: Int, length: Int) {
    val fd = urandomFd()
    dst.usePinned { pinned ->
        var done = 0
        while (done < length) {
            val n = platform.posix.read(fd, pinned.addressOf(offset + done), (length - done).convert()).toInt()
            if (n < 0) { if (errno == EINTR) continue; error("reading /dev/urandom failed (errno=$errno)") }
            check(n > 0) { "/dev/urandom returned EOF" }
            done += n
        }
    }
}
