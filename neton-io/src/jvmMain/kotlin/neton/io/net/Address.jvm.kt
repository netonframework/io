package neton.io.net

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

// A [SockAddr]'s bytes are the platform's `sockaddr`. The JVM has none, so it uses Linux's layout:
// sockaddr_in (family 2 LE, port 2 BE, address 4, zero 8) and sockaddr_in6 (family 10 LE, port 2 BE,
// flow info 4, address 16, scope id 4). Code that reads the bytes sees the same shapes as on Linux.

private const val AF_INET = 2
private const val AF_INET6 = 10

private fun sockAddr(address: InetAddress, port: Int): SockAddr {
    val ip = address.address
    val v6 = ip.size == 16
    val bytes = ByteArray(if (v6) 28 else 16)
    bytes[0] = (if (v6) AF_INET6 else AF_INET).toByte()
    bytes[2] = (port ushr 8).toByte(); bytes[3] = port.toByte()
    if (v6) {
        ip.copyInto(bytes, 8)
        val scope = (address as Inet6Address).scopeId
        for (i in 0 until 4) bytes[24 + i] = (scope ushr (8 * i)).toByte()   // host order, as on Linux
    } else {
        ip.copyInto(bytes, 4)
    }
    return SockAddr(if (v6) AF_INET6 else AF_INET, v6, bytes)
}

private fun SockAddr.toInet(): InetSocketAddress {
    val port = ((bytes[2].toInt() and 0xff) shl 8) or (bytes[3].toInt() and 0xff)
    if (!isIpv6) return InetSocketAddress(InetAddress.getByAddress(bytes.copyOfRange(4, 8)), port)
    var scope = 0
    for (i in 3 downTo 0) scope = (scope shl 8) or (bytes[24 + i].toInt() and 0xff)
    val ip = bytes.copyOfRange(8, 24)
    return InetSocketAddress(if (scope != 0) Inet6Address.getByAddress(null, ip, scope) else InetAddress.getByAddress(ip), port)
}

/** The 16-byte form of [address] on an IPv6-family socket: IPv4 becomes `::ffff:a.b.c.d`. */
internal fun ipv6Bytes(address: InetAddress): ByteArray {
    val ip = address.address
    if (ip.size == 16) return ip
    return ByteArray(16).also { ip.copyInto(it, 12); it[10] = -1; it[11] = -1 }
}

/** An IPv4 or IPv6 literal, which the JDK parses without a lookup. */
private fun isLiteral(host: String): Boolean =
    ':' in host || (host.isNotEmpty() && host.all { it.isDigit() || it == '.' } && host.count { it == '.' } == 3)

internal actual suspend fun resolve(host: String, port: Int, passive: Boolean): List<SockAddr> {
    require(port in 0..65535) { "port out of range: $port" }
    val addresses = try {
        if (isLiteral(host)) arrayOf(InetAddress.getByName(host.removePrefix("[").removeSuffix("]")))
        // SPEC §27.5: a lookup blocks, so it runs on the IO pool, never on a reactor thread; the
        // caller resumes on its own reactor.
        else withContext(Dispatchers.IO) { InetAddress.getAllByName(host) }
    } catch (e: UnknownHostException) {
        throw ResolveException("cannot resolve '$host': ${e.message ?: "unknown host"}")
    } catch (e: SecurityException) {
        throw ResolveException("cannot resolve '$host': ${e.message}")
    }
    if (addresses.isEmpty()) throw ResolveException("cannot resolve '$host': no addresses")
    return addresses.map { sockAddr(it, port) }
}

internal actual fun tcpListenAddr(addr: SockAddr, display: String, options: SocketOptions): Int {
    val channel = ServerSocketChannel.open()
    val fd = Channels.add(channel)
    try {
        applyListenerOptions(fd, options)
        // ServerSocket.bind rather than the channel's: Android has the latter only from API 24.
        // A JDK socket bound to "::" is dual-stack, as the native listener is with IPV6_V6ONLY off.
        channel.socket().bind(addr.toInet(), options.backlog)
        channel.configureBlocking(false)
        if (addr.isIpv6) Channels.markIpv6(fd)
    } catch (e: IOException) {
        val code = JvmErrno.codeOf(e)
        closeFd(fd)
        error("bind($display) failed: ${e.message ?: errnoMessage(code)} (errno=$code)")
    }
    return fd
}

internal actual fun tcpConnectAddr(addr: SockAddr, display: String, options: SocketOptions): Int {
    val channel = SocketChannel.open()
    val fd = Channels.add(channel)
    try {
        channel.configureBlocking(false)
        if (addr.isIpv6) Channels.markIpv6(fd)
        applyStreamOptions(fd, options)          // before connect: buffer sizes fix the window scale
        // true when the connect completed at once (loopback can); the reactor then sees it writable.
        channel.connect(addr.toInet())
    } catch (e: Exception) {
        // IOException, or UnresolvedAddressException / UnsupportedAddressTypeException (runtime).
        val code = JvmErrno.codeOf(e)
        closeFd(fd)
        throw ConnectException("connect to $display failed: ${errnoMessage(code)} (errno $code)").also { it.code = code }
    }
    return fd
}

internal actual fun setNoDelay(fd: Int) {
    try { (Channels[fd] as? SocketChannel)?.socket()?.tcpNoDelay = true } catch (_: IOException) { }
}

/** 0 with out[0] family, out[1] port, out[2] scope and [ip]; or -errno. */
internal actual fun udpLocal(fd: Int, out: IntArray, ip: ByteArray): Int {
    val (address, port) = when (val channel = Channels[fd]) {
        is ServerSocketChannel -> channel.socket().inetAddress to channel.socket().localPort
        is SocketChannel -> channel.socket().localAddress to channel.socket().localPort
        is java.nio.channels.DatagramChannel -> channel.socket().localAddress to channel.socket().localPort
        else -> return -JvmErrno.EBADF
    }
    if (address == null || port <= 0) return -JvmErrno.ENOTCONN
    val bytes = if (Channels.isIpv6(fd)) ipv6Bytes(address) else address.address
    bytes.copyInto(ip)
    out[0] = if (bytes.size == 16) 6 else 4
    out[1] = port
    out[2] = (address as? Inet6Address)?.scopeId ?: 0
    return 0
}

internal actual fun socketAddress(fd: Int, peer: Boolean): SocketAddress? {
    val (address, port) = when (val channel = Channels[fd]) {
        // A listener has only a local address (TcpListener.localAddress).
        is ServerSocketChannel -> if (peer) return null else channel.socket().let { (it.inetAddress ?: return null) to it.localPort }
        is SocketChannel -> channel.socket().let { socket ->
            ((if (peer) socket.inetAddress else socket.localAddress) ?: return null) to (if (peer) socket.port else socket.localPort)
        }
        else -> return null
    }
    if (port <= 0) return null
    val bytes = if (Channels.isIpv6(fd)) ipv6Bytes(address) else address.address
    return SocketAddress.of(bytes, port, (address as? Inet6Address)?.scopeId ?: 0)
}
