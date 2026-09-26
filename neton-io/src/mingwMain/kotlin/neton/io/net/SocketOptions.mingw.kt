@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import neton.io.win.neton_getsockopt_int
import neton.io.win.neton_setsockopt_int
import neton.io.win.neton_setsockopt_linger
import platform.posix.IPPROTO_TCP
import platform.posix.SOL_SOCKET
import platform.posix.SO_KEEPALIVE
import platform.posix.SO_RCVBUF
import platform.posix.SO_SNDBUF
import platform.posix.TCP_NODELAY

// Windows 10 (1709+) keepalive options on IPPROTO_TCP (ws2ipdef.h).
private const val TCP_KEEPIDLE_WIN = 3
private const val TCP_KEEPCNT_WIN = 16
private const val TCP_KEEPINTVL_WIN = 17

private fun getInt(fd: Int, level: Int, name: Int): Int = memScoped {
    val v = alloc<IntVar>()
    if (neton_getsockopt_int(fd.toSocket(), level, name, v.ptr) != 0) -1 else v.value
}

/** Windows: no SO_REUSEADDR (it would let another socket take over a port in use) and no SO_REUSEPORT. */
internal actual fun applyListenerOptions(fd: Int, options: SocketOptions) {
    if (options.receiveBufferSize > 0) neton_setsockopt_int(fd.toSocket(), SOL_SOCKET, SO_RCVBUF, options.receiveBufferSize)
}

internal actual fun applyStreamOptions(fd: Int, options: SocketOptions) {
    val s = fd.toSocket()
    if (options.noDelay) neton_setsockopt_int(s, IPPROTO_TCP, TCP_NODELAY, 1)
    options.keepAlive?.let { k ->
        neton_setsockopt_int(s, SOL_SOCKET, SO_KEEPALIVE, 1)
        neton_setsockopt_int(s, IPPROTO_TCP, TCP_KEEPIDLE_WIN, k.idleSeconds)
        neton_setsockopt_int(s, IPPROTO_TCP, TCP_KEEPINTVL_WIN, k.intervalSeconds)
        neton_setsockopt_int(s, IPPROTO_TCP, TCP_KEEPCNT_WIN, k.probes)
    }
    if (options.sendBufferSize > 0) neton_setsockopt_int(s, SOL_SOCKET, SO_SNDBUF, options.sendBufferSize)
    if (options.receiveBufferSize > 0) neton_setsockopt_int(s, SOL_SOCKET, SO_RCVBUF, options.receiveBufferSize)
    if (options.lingerSeconds >= 0) neton_setsockopt_linger(s, 1, options.lingerSeconds)
}

internal actual fun readSocketOptions(fd: Int): SocketOptionsSnapshot = SocketOptionsSnapshot(
    noDelay = getInt(fd, IPPROTO_TCP, TCP_NODELAY) != 0,
    keepAlive = getInt(fd, SOL_SOCKET, SO_KEEPALIVE) != 0,
    keepIdleSeconds = getInt(fd, IPPROTO_TCP, TCP_KEEPIDLE_WIN),
    keepIntervalSeconds = getInt(fd, IPPROTO_TCP, TCP_KEEPINTVL_WIN),
    keepProbes = getInt(fd, IPPROTO_TCP, TCP_KEEPCNT_WIN),
    sendBufferSize = getInt(fd, SOL_SOCKET, SO_SNDBUF),
    receiveBufferSize = getInt(fd, SOL_SOCKET, SO_RCVBUF),
    reuseAddress = false,
    reusePort = false,
)
