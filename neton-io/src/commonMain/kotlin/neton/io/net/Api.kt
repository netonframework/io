package neton.io.net

import kotlinx.coroutines.CoroutineScope
import neton.io.core.IoStream

/**
 * Public entry points for consumers of the reactor.
 *
 * The reactor, drivers and socket streams stay internal; callers work through these functions
 * and the [IoStream] interface from neton-io-core. The active driver (readiness or completion)
 * is chosen by NETON_IO_DRIVER and the platform.
 */

/** Run [block] on a fresh reactor until it completes. Everything below must run inside it. */
fun runReactor(block: suspend CoroutineScope.() -> Unit) = Reactor.run(block)

/** A listening TCP endpoint. */
class TcpListener internal constructor(private val server: TcpServer) {
    suspend fun accept(): IoStream = server.accept()
    fun close() = server.close()

    /** The address the listener is bound to — the port the OS chose for port 0. */
    val localAddress: SocketAddress
        // getsockname through the TCP path: the UDP helper it used is not implemented on Windows.
        get() = checkNotNull(socketAddress(server.listenFd, peer = false)) { "getsockname failed" }
}

/**
 * The remote address of a TCP connection (getpeername), or null: for streams that are not sockets (memory streams,
 * wrappers such as a TLS stream — take the address from the socket before wrapping it), for Unix sockets, and once the
 * stream is closed or the peer is gone (ENOTCONN). A server reads it right after `accept`.
 */
val neton.io.core.IoStream.peerAddress: SocketAddress?
    get() = (this as? ReactorStream)?.takeUnless { it.isClosed }?.let { socketAddress(it.fd, peer = true) }

/** The local address of a TCP connection (getsockname), or null as for [peerAddress]. */
val neton.io.core.IoStream.localAddress: SocketAddress?
    get() = (this as? ReactorStream)?.takeUnless { it.isClosed }?.let { socketAddress(it.fd, peer = false) }

/**
 * The peer reset the connection (ECONNRESET / WSAECONNRESET): after a close handshake a protocol may treat this as a
 * normal end (tungstenite `check_connection_reset`).
 */
val neton.io.core.IoException.isConnectionReset: Boolean get() = errno != 0 && errno == connectionResetErrno

internal expect val connectionResetErrno: Int

/** Bind and listen on [host]:[port] with [options] (also applied to accepted connections). Must run inside [runReactor]. */
suspend fun listen(host: String, port: Int, options: SocketOptions = SocketOptions.Default): TcpListener =
    TcpListener(listenTcpServer(host, port, options))

/** Connect to [host]:[port] with [options]. Throws [ConnectException] if the connection cannot be established (or times out). */
suspend fun connect(host: String, port: Int, options: SocketOptions = SocketOptions.Default): IoStream =
    connectStream(host, port, options)

/** A TCP connect that failed (refused, unreachable, timed out by the OS). */
class ConnectException(message: String) : Exception(message) {
    /** The platform error code (errno / Winsock), 0 if none; lets Unix-socket connect tell "refused" from "retry". */
    internal var code: Int = 0
}
