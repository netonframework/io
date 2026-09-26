package neton.io.net

import kotlinx.coroutines.delay
import neton.io.core.IoStream
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlin.time.TimeSource

// Unix domain stream sockets (SPEC §23.6). They reuse the TCP listen / connect / accept paths with
// an AF_UNIX address; only the sockaddr_un layout and a few error codes are per platform.

/** AF_UNIX on this platform. */
internal expect val AF_UNIX_FAMILY: Int
/** Apple's sockaddr_un starts with a one-byte sun_len, then a one-byte family; elsewhere the family is 16 bits. */
internal expect val SUN_HAS_LEN: Boolean
/** Size of sun_path: 104 on Apple, 108 on Linux / Android / Windows. */
internal expect val SUN_PATH_SIZE: Int
/** Linux and Android have the abstract namespace (a name starting with NUL, written here as `@name`). */
internal expect val UNIX_ABSTRACT_SUPPORTED: Boolean
/** The connect error code for "nobody listens on this socket file" (ECONNREFUSED / WSAECONNREFUSED). */
internal expect val CONNECT_REFUSED_CODE: Int
/** The connect error code for "listener backlog full, try again" (EAGAIN on POSIX; none on Windows). */
internal expect val CONNECT_RETRY_CODE: Int
/** Remove a socket file; false if it could not be removed. */
internal expect fun removeSocketFile(path: String): Boolean

/**
 * The sockaddr_un bytes for [path]. A leading `@` selects the Linux abstract namespace. Throws
 * [IllegalArgumentException] for an empty path, an embedded NUL, or a path longer than sun_path holds.
 */
internal fun unixSockAddr(path: String): SockAddr {
    require(path.isNotEmpty()) { "unix socket path is empty" }
    val abstract = path.startsWith('@')
    require(!abstract || UNIX_ABSTRACT_SUPPORTED) { "abstract unix sockets ('$path') are Linux / Android only" }
    val name = (if (abstract) path.substring(1) else path).encodeToByteArray()
    require(name.none { it == 0.toByte() }) { "unix socket path contains NUL" }
    // A file path needs its NUL terminator; an abstract name needs its leading NUL. Either way one byte.
    require(name.size <= SUN_PATH_SIZE - 1) { "unix socket path too long: ${name.size} bytes, the limit is ${SUN_PATH_SIZE - 1}" }
    val len = 2 + 1 + name.size
    val b = ByteArray(len)
    if (SUN_HAS_LEN) {
        b[0] = len.toByte(); b[1] = AF_UNIX_FAMILY.toByte()
    } else {
        // 16-bit family in host order; every supported target is little-endian.
        b[0] = (AF_UNIX_FAMILY and 0xFF).toByte(); b[1] = (AF_UNIX_FAMILY shr 8).toByte()
    }
    name.copyInto(b, if (abstract) 3 else 2)
    return SockAddr(AF_UNIX_FAMILY, false, b)
}

/** TCP-only options dropped: Nagle, keepalive and port reuse do not exist for AF_UNIX. */
internal fun SocketOptions.forUnix(): SocketOptions = SocketOptions(
    noDelay = false, keepAlive = null, sendBufferSize = sendBufferSize, receiveBufferSize = receiveBufferSize,
    backlog = backlog, reuseAddress = false, reusePort = false, lingerSeconds = lingerSeconds,
    connectTimeoutMillis = connectTimeoutMillis,
)

/**
 * Bind [path]. If that fails because the file exists, probe it: a socket file nobody listens on
 * (left behind by a crashed process) is removed and the bind retried; a live listener is an error,
 * since binding over it would steal its path. Probing only after a failed bind keeps the common
 * case free of the extra connect (a probe that reaches a live listener shows up in its accept queue).
 */
private fun bindUnix(addr: SockAddr, path: String, opts: SocketOptions): Int {
    val display = "unix:$path"
    val first = try { return tcpListenAddr(addr, display, opts) } catch (e: IllegalStateException) { e }
    val probe = try {
        tcpConnectAddr(addr, display, opts)
    } catch (e: ConnectException) {
        if (e.code != CONNECT_REFUSED_CODE || !removeSocketFile(path)) throw first
        return tcpListenAddr(addr, display, opts)
    }
    closeFd(probe)
    error("bind($display) failed: another process is listening on it")
}

internal suspend fun listenUnixServer(path: String, options: SocketOptions): TcpServer {
    val addr = unixSockAddr(path)
    val opts = options.forUnix()
    val fd = bindUnix(addr, path, opts)
    return TcpServer(fd, currentReactor(), opts).also { s -> if (!path.startsWith('@')) s.afterClose = { removeSocketFile(path) } }
}

/**
 * Connect to the Unix socket at [path]. A full listener backlog (EAGAIN) is retried with backoff
 * until the connect timeout (10 s when [SocketOptions.connectTimeoutMillis] is 0).
 */
internal suspend fun connectUnixStream(path: String, options: SocketOptions): IoStream {
    val reactor = currentReactor()
    val addr = unixSockAddr(path)
    val opts = options.forUnix()
    val display = "unix:$path"
    val limit = if (opts.connectTimeoutMillis > 0) opts.connectTimeoutMillis else 10_000L
    val start = TimeSource.Monotonic.markNow()
    var backoff = 1L
    while (true) {
        val fd = try {
            tcpConnectAddr(addr, display, opts)
        } catch (e: ConnectException) {
            if (e.code != CONNECT_RETRY_CODE || start.elapsedNow().inWholeMilliseconds >= limit) throw e
            delay(backoff); backoff = minOf(backoff * 2, 50L)
            continue
        }
        return finishConnect(reactor, fd, display, opts)
    }
}

/** A listening Unix domain socket; closing it removes its socket file. */
class UnixListener internal constructor(private val server: TcpServer) {
    suspend fun accept(): IoStream = server.accept()
    fun close() = server.close()
}

/**
 * Listen on the Unix domain socket [path] (a leading `@` is the Linux abstract namespace). A socket
 * file nobody listens on is replaced; one with a live listener is an error. Must run inside [runReactor].
 */
suspend fun listenUnix(path: String, options: SocketOptions = SocketOptions.Default): UnixListener =
    UnixListener(listenUnixServer(path, options))

/** Connect to the Unix domain socket [path]. Throws [ConnectException] on failure. */
suspend fun connectUnix(path: String, options: SocketOptions = SocketOptions.Default): IoStream =
    connectUnixStream(path, options)

/**
 * [listenGroup] for a Unix domain socket: reactor 0 accepts and hands connections to [reactors]
 * reactors (SO_REUSEPORT does not apply to AF_UNIX, so the mode is always [AcceptMode.Handoff]).
 */
suspend fun listenUnixGroup(
    path: String,
    reactors: Int = cpuCount(),
    options: SocketOptions = SocketOptions.Default,
    maxConnections: Int = 0,
): TcpServerGroup {
    require(reactors >= 1) { "reactors must be >= 1" }
    require(maxConnections >= 0) { "maxConnections must be >= 0" }
    val localScope = CoroutineScope(coroutineContext)
    val listeners = arrayOfNulls<TcpServer>(reactors)
    listeners[0] = listenUnixServer(path, options)
    val group = ReactorGroup(reactors)
    group.start(currentReactor(), localScope)
    return TcpServerGroup(listeners, group, reactors, AcceptMode.Handoff, maxConnections)
}
