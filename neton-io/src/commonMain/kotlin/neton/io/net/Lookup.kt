package neton.io.net

import neton.io.core.IoException

/** [lookupHost] found no address for a host. */
class UnknownHostException(val host: String, message: String) : IoException(message)

/**
 * Resolve [host] to socket addresses with [port], in the resolver's order (RFC 6724), like tokio's
 * `net::lookup_host`: for protocols that dial addresses themselves (UDP, QUIC). IP literals (`127.0.0.1`, `::1`, also
 * bracketed) resolve inline without blocking; names are looked up off the reactor thread, as [connect] does.
 *
 * @throws UnknownHostException when the name does not resolve.
 */
suspend fun lookupHost(host: String, port: Int): List<SocketAddress> {
    require(port in 0..65535) { "port out of range: $port" }
    val name = if (host.length > 2 && host.startsWith('[') && host.endsWith(']')) host.substring(1, host.length - 1) else host
    val addrs = try {
        resolve(name, port, passive = false)
    } catch (e: ResolveException) {
        throw UnknownHostException(host, e.message ?: "cannot resolve '$host'")
    }
    return addrs.mapNotNull { it.toSocketAddress() }
}

/**
 * The IP address of a resolved `sockaddr`. Every platform here (and the JVM's Linux-layout bytes, see Address.jvm.kt)
 * puts the port (big-endian) at offset 2, an IPv4 address at offset 4, and an IPv6 address at offset 8 with its scope
 * id (host order, little-endian on every supported target) at offset 24; only the family field before the port
 * differs. Anything else (a Unix socket) has no IP address.
 */
internal fun SockAddr.toSocketAddress(): SocketAddress? {
    val b = bytes
    if (b.size < 8) return null
    val port = ((b[2].toInt() and 0xff) shl 8) or (b[3].toInt() and 0xff)
    if (!isIpv6) return SocketAddress.of(b.copyOfRange(4, 8), port)
    if (b.size < 28) return null
    val scope = (b[24].toInt() and 0xff) or ((b[25].toInt() and 0xff) shl 8) or
        ((b[26].toInt() and 0xff) shl 16) or ((b[27].toInt() and 0xff) shl 24)
    return SocketAddress.of(b.copyOfRange(8, 24), port, scope)
}
