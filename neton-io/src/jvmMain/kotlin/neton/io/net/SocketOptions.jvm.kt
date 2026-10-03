package neton.io.net

import java.net.StandardSocketOptions
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

// Through java.net.Socket / ServerSocket setters, which Android has had since API 1; the channel
// setOption API is Android API 24. Keepalive timing (idle / interval / probes) has no portable JDK
// API (jdk.net.ExtendedSocketOptions is JDK 11+ and absent on Android): only keepalive on/off is
// applied, and the OS supplies the timings. SO_REUSEPORT is best effort for the same reason.

internal actual fun applyListenerOptions(fd: Int, options: SocketOptions) {
    val channel = Channels[fd] as? ServerSocketChannel ?: return
    val socket = channel.socket()
    socket.reuseAddress = options.reuseAddress
    if (options.receiveBufferSize > 0) socket.receiveBufferSize = options.receiveBufferSize
    if (options.reusePort) runCatching { channel.setOption(StandardSocketOptions.SO_REUSEPORT, true) }
}

internal actual fun applyStreamOptions(fd: Int, options: SocketOptions) {
    val socket = (Channels[fd] as? SocketChannel)?.socket() ?: return
    socket.tcpNoDelay = options.noDelay
    if (options.keepAlive != null) socket.keepAlive = true
    if (options.sendBufferSize > 0) socket.sendBufferSize = options.sendBufferSize
    if (options.receiveBufferSize > 0) socket.receiveBufferSize = options.receiveBufferSize
    if (options.lingerSeconds >= 0) socket.setSoLinger(true, options.lingerSeconds)
}

internal actual fun readSocketOptions(fd: Int): SocketOptionsSnapshot {
    when (val channel = Channels[fd]) {
        is SocketChannel -> {
            val s = channel.socket()
            return SocketOptionsSnapshot(
                noDelay = s.tcpNoDelay, keepAlive = s.keepAlive,
                keepIdleSeconds = 0, keepIntervalSeconds = 0, keepProbes = 0,
                sendBufferSize = s.sendBufferSize, receiveBufferSize = s.receiveBufferSize,
                reuseAddress = s.reuseAddress,
                reusePort = runCatching { channel.getOption(StandardSocketOptions.SO_REUSEPORT) }.getOrNull() ?: false,
            )
        }
        is ServerSocketChannel -> {
            val s = channel.socket()
            return SocketOptionsSnapshot(
                noDelay = false, keepAlive = false, keepIdleSeconds = 0, keepIntervalSeconds = 0, keepProbes = 0,
                sendBufferSize = 0, receiveBufferSize = s.receiveBufferSize, reuseAddress = s.reuseAddress,
                reusePort = runCatching { channel.getOption(StandardSocketOptions.SO_REUSEPORT) }.getOrNull() ?: false,
            )
        }
        else -> error("fd $fd is not a TCP socket")
    }
}
