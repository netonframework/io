package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.value
import platform.posix.SOL_SOCKET
import platform.posix.SO_NOSIGPIPE
import platform.posix.pthread_self
import platform.posix.setsockopt

/** Apple: SO_NOSIGPIPE per socket; send(2) then returns EPIPE instead of raising SIGPIPE. */
@OptIn(ExperimentalForeignApi::class)
internal actual fun suppressSigpipe(fd: Int) = memScoped {
    val one = alloc<IntVar>()
    one.value = 1
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, one.ptr, sizeOf<IntVar>().convert())
    Unit
}

internal actual val SEND_FLAGS: Int = 0

@OptIn(ExperimentalForeignApi::class)
internal actual fun currentThreadId(): ULong = pthread_self()!!.rawValue.toLong().toULong()
