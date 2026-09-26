package neton.io.net

// Address resolution and TCP socket setup, one implementation per platform family (SPEC §18.2, §20).

/**
 * One resolved socket address: the platform's address family (its numeric value differs per OS; a
 * SockAddr is produced and consumed on the same platform), whether it is IPv6, and the raw `sockaddr`
 * bytes exactly as the platform resolver produced them (SPEC §18.2). Keeping the bytes opaque lets IPv4 and IPv6, and
 * every platform's `sockaddr` layout, share one code path.
 */
internal class SockAddr(val family: Int, val isIpv6: Boolean, val bytes: ByteArray)

/** A host that could not be resolved (or an address that is not valid for the requested use). */
internal class ResolveException(message: String) : Exception(message)

/**
 * Resolve [host]:[port] to socket addresses, in the resolver's order (RFC 6724). Literals resolve
 * inline without blocking; names are looked up off the reactor thread.
 */
internal expect suspend fun resolve(host: String, port: Int, passive: Boolean): List<SockAddr>

/** Open, bind and listen on [addr]; non-blocking. `::` listens dual-stack. */
internal expect fun tcpListenAddr(addr: SockAddr, display: String, options: SocketOptions = SocketOptions.Default): Int

/** Open a socket for [addr], apply [options], and start a non-blocking connect; throws [ConnectException] on immediate failure. */
internal expect fun tcpConnectAddr(addr: SockAddr, display: String, options: SocketOptions = SocketOptions.Default): Int

/** Disable Nagle on a TCP stream (SPEC §19.6). */
internal expect fun setNoDelay(fd: Int)

/** `host:port`, with IPv6 literals bracketed. */
internal fun hostPort(host: String, port: Int): String = if (':' in host) "[$host]:$port" else "$host:$port"
