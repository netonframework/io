package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import platform.posix.strerror
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.value
import platform.posix.AF_INET
import platform.posix.EINPROGRESS
import platform.posix.errno
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_REUSEADDR
import platform.posix.bind
import platform.posix.connect
import platform.posix.listen
import platform.posix.sockaddr
import platform.posix.sockaddr_in
import platform.posix.socket
import platform.posix.setsockopt

@OptIn(ExperimentalForeignApi::class)
internal actual fun tcpListen(host: String, port: Int, backlog: Int): Int = memScoped {
    val fd = socket(AF_INET, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed: ${strerror(errno)?.toKString()} (errno=$errno)" }

    val one = alloc<IntVar>()
    one.value = 1
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, one.ptr, sizeOf<IntVar>().convert())

    val addr = alloc<sockaddr_in>()
    addr.sin_family = AF_INET.convert()
    addr.sin_port = htons(port.toUShort())
    addr.sin_addr.s_addr = ipv4NetworkOrder(host)

    check(bind(fd, addr.ptr.reinterpret<sockaddr>(), sizeOf<sockaddr_in>().convert()) == 0) { "bind($host:$port) failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    check(listen(fd, backlog) == 0) { "listen($host:$port) failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    setNonBlocking(fd)
    fd
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun tcpConnect(host: String, port: Int): Int = memScoped {
    val fd = socket(AF_INET, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    setNonBlocking(fd)
    suppressSigpipe(fd)

    val addr = alloc<sockaddr_in>()
    addr.sin_family = AF_INET.convert()
    addr.sin_port = htons(port.toUShort())
    addr.sin_addr.s_addr = ipv4NetworkOrder(host)

    // Non-blocking connect returns -1/EINPROGRESS; completion is observed as writability.
    val rc = connect(fd, addr.ptr.reinterpret<sockaddr>(), sizeOf<sockaddr_in>().convert())
    if (rc != 0 && errno != EINPROGRESS) {
        val err = errno
        closeFd(fd)
        throw ConnectException("connect to $host:$port failed: ${errnoMessage(err)} (errno $err)")
    }
    fd
}
