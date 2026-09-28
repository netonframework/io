@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.value
import platform.posix.IPPROTO_TCP
import platform.posix.SOL_SOCKET
import platform.posix.SO_KEEPALIVE
import platform.posix.SO_LINGER
import platform.posix.SO_RCVBUF
import platform.posix.SO_REUSEADDR
import platform.posix.SO_REUSEPORT
import platform.posix.SO_SNDBUF
import platform.posix.TCP_NODELAY

/** Keepalive option names differ: Linux/Android TCP_KEEPIDLE, Apple TCP_KEEPALIVE (actuals in epollMain / appleMain). */
internal expect val TCP_KEEP_IDLE_OPTION: Int
internal expect val TCP_KEEP_INTERVAL_OPTION: Int
internal expect val TCP_KEEP_COUNT_OPTION: Int

internal fun setIntOption(fd: Int, level: Int, name: Int, value: Int) {
    neton.io.posixshim.neton_setsockopt_int(fd, level, name, value)
}

internal fun getIntOption(fd: Int, level: Int, name: Int): Int = memScoped {
    val v = alloc<IntVar>()
    if (neton.io.posixshim.neton_getsockopt_int(fd, level, name, v.ptr) != 0) -1 else v.value
}

internal actual fun applyListenerOptions(fd: Int, options: SocketOptions) {
    if (options.reuseAddress) setIntOption(fd, SOL_SOCKET, SO_REUSEADDR, 1)
    if (options.reusePort) setIntOption(fd, SOL_SOCKET, SO_REUSEPORT, 1)
    if (options.receiveBufferSize > 0) setIntOption(fd, SOL_SOCKET, SO_RCVBUF, options.receiveBufferSize)
}

internal actual fun applyStreamOptions(fd: Int, options: SocketOptions) {
    if (options.noDelay) setIntOption(fd, IPPROTO_TCP, TCP_NODELAY, 1)
    options.keepAlive?.let { k ->
        setIntOption(fd, SOL_SOCKET, SO_KEEPALIVE, 1)
        setIntOption(fd, IPPROTO_TCP, TCP_KEEP_IDLE_OPTION, k.idleSeconds)
        setIntOption(fd, IPPROTO_TCP, TCP_KEEP_INTERVAL_OPTION, k.intervalSeconds)
        setIntOption(fd, IPPROTO_TCP, TCP_KEEP_COUNT_OPTION, k.probes)
    }
    if (options.sendBufferSize > 0) setIntOption(fd, SOL_SOCKET, SO_SNDBUF, options.sendBufferSize)
    if (options.receiveBufferSize > 0) setIntOption(fd, SOL_SOCKET, SO_RCVBUF, options.receiveBufferSize)
    if (options.lingerSeconds >= 0) neton.io.posixshim.neton_setsockopt_linger(fd, 1, options.lingerSeconds)
}

internal actual fun readSocketOptions(fd: Int): SocketOptionsSnapshot = SocketOptionsSnapshot(
    noDelay = getIntOption(fd, IPPROTO_TCP, TCP_NODELAY) != 0,
    keepAlive = getIntOption(fd, SOL_SOCKET, SO_KEEPALIVE) != 0,
    keepIdleSeconds = getIntOption(fd, IPPROTO_TCP, TCP_KEEP_IDLE_OPTION),
    keepIntervalSeconds = getIntOption(fd, IPPROTO_TCP, TCP_KEEP_INTERVAL_OPTION),
    keepProbes = getIntOption(fd, IPPROTO_TCP, TCP_KEEP_COUNT_OPTION),
    sendBufferSize = getIntOption(fd, SOL_SOCKET, SO_SNDBUF),
    receiveBufferSize = getIntOption(fd, SOL_SOCKET, SO_RCVBUF),
    reuseAddress = getIntOption(fd, SOL_SOCKET, SO_REUSEADDR) != 0,
    reusePort = getIntOption(fd, SOL_SOCKET, SO_REUSEPORT) != 0,
)
