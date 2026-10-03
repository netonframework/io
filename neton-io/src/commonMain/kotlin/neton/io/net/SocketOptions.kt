package neton.io.net

/** TCP keepalive probing (SPEC §23.5): idle time before the first probe, interval between probes, probes before giving up. */
class KeepAlive(val idleSeconds: Int, val intervalSeconds: Int, val probes: Int) {
    init { require(idleSeconds > 0 && intervalSeconds > 0 && probes > 0) { "keepalive values must be positive" } }
}

/**
 * Socket options for [listen], [listenGroup] and [connect] (SPEC §23.5). A listener's options also
 * apply to every connection it accepts. Defaults are what neton-io did before options existed.
 *
 * Platform notes: Windows ignores [reuseAddress] (there it would let another socket take over a port
 * in use) and [reusePort]; macOS accepts [reusePort] but does not load-balance between the sockets.
 */
class SocketOptions(
    /** Disable Nagle (SPEC §19.6). */
    val noDelay: Boolean = true,
    /** TCP keepalive probing; null leaves the OS default (off). */
    val keepAlive: KeepAlive? = null,
    /** SO_SNDBUF in bytes; 0 leaves the OS default (and its autotuning). */
    val sendBufferSize: Int = 0,
    /** SO_RCVBUF in bytes; 0 leaves the OS default (and its autotuning). */
    val receiveBufferSize: Int = 0,
    /**
     * Listen backlog; the kernel caps it (Linux: net.core.somaxconn, 4096 since 5.4). SPEC §26.6: at
     * 1024 a burst of connects overflowed the accept queue and each dropped SYN cost the client 1 s.
     */
    val backlog: Int = 4096,
    /** SO_REUSEADDR on listeners (POSIX). */
    val reuseAddress: Boolean = true,
    /** SO_REUSEPORT on listeners: several sockets may bind the same port (Linux load-balances them). */
    val reusePort: Boolean = false,
    /** SO_LINGER seconds on close; negative leaves the OS default. 0 makes close send RST. */
    val lingerSeconds: Int = -1,
    /** Per-address connect timeout in ms for [connect]; 0 waits as long as the OS does. */
    val connectTimeoutMillis: Long = 0,
) {
    /** A copy with SO_REUSEPORT on (used by [AcceptMode.ReusePort]). */
    internal fun withReusePort(): SocketOptions = SocketOptions(
        noDelay, keepAlive, sendBufferSize, receiveBufferSize, backlog, reuseAddress, reusePort = true,
        lingerSeconds = lingerSeconds, connectTimeoutMillis = connectTimeoutMillis,
    )

    companion object {
        val Default = SocketOptions()
    }
}

/** Options set on a listening socket before bind: address/port reuse, receive buffer (window scaling is fixed at bind). */
internal expect fun applyListenerOptions(fd: Int, options: SocketOptions)

/** Options set on a connected or accepted stream socket. */
internal expect fun applyStreamOptions(fd: Int, options: SocketOptions)

/** What is actually set on [fd] (read back with getsockopt), for tests and diagnostics. */
internal class SocketOptionsSnapshot(
    val noDelay: Boolean,
    val keepAlive: Boolean,
    val keepIdleSeconds: Int,
    val keepIntervalSeconds: Int,
    val keepProbes: Int,
    val sendBufferSize: Int,
    val receiveBufferSize: Int,
    val reuseAddress: Boolean,
    val reusePort: Boolean,
)

internal expect fun readSocketOptions(fd: Int): SocketOptionsSnapshot
