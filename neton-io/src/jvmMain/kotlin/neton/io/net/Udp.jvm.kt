package neton.io.net

import java.io.IOException
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.PortUnreachableException
import java.net.StandardProtocolFamily
import java.net.StandardSocketOptions
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel

// UDP on the JVM (SPEC §37): a non-blocking DatagramChannel in the fd table ([Channels]), waited on by the NIO reactor
// like any channel (OP_READ / OP_WRITE). What NIO cannot do is declared, not emulated: one datagram per receive, no
// GSO or GRO (1 segment), no per-datagram ECN (none reported or sent), no choice of source address, no destination
// address, and no IPV6_V6ONLY (an IPv6 socket is dual-stack, the JDK's default). QUIC runs without all of these.

internal actual class RecvBatchPins actual constructor(batch: RecvBatch) {
    actual fun unpin() {}
}

internal actual class TransmitPins actual constructor(transmit: Transmit) {
    actual fun unpin() {}
}

internal actual fun udpBatchSize(): Int = 1

internal actual fun udpBind(family: Int, ip: ByteArray, port: Int, scope: Int, v6only: Boolean): Int {
    val channel = try {
        DatagramChannel.open(if (family == 6) StandardProtocolFamily.INET6 else StandardProtocolFamily.INET)
    } catch (e: IOException) {
        return -JvmErrno.record(e)
    }
    return try {
        channel.configureBlocking(false)
        channel.bind(InetSocketAddress(inetAddress(family, ip, 0, scope), port))
        val id = Channels.add(channel)
        if (family == 6) Channels.markIpv6(id)
        id
    } catch (e: Exception) {
        try { channel.close() } catch (_: IOException) { }
        -JvmErrno.record(e)
    }
}

/** May fragment (no don't-fragment control on the JVM); GSO and GRO 1. */
internal actual fun udpSetup(fd: Int, v6: Boolean, v6only: Boolean, caps: IntArray): Int {
    caps[0] = 1
    caps[1] = 1
    return 1
}

/** [which] 0 send, 1 receive buffer; [value] < 0 reads it. */
internal actual fun udpBuffer(fd: Int, which: Int, value: Int): Int {
    val channel = Channels[fd] as? DatagramChannel ?: return -JvmErrno.EBADF
    val option = if (which == 0) StandardSocketOptions.SO_SNDBUF else StandardSocketOptions.SO_RCVBUF
    return try {
        if (value >= 0) channel.setOption(option, value)
        channel.getOption(option)
    } catch (e: IOException) {
        -JvmErrno.record(e)
    }
}

internal actual fun udpRecv(fd: Int, batch: RecvBatch): Int {
    val channel = Channels[fd] as? DatagramChannel ?: return -(1000 + JvmErrno.EBADF)
    val view = ByteBuffer.wrap(batch.buffer, 0, batch.slotSize)
    val from = try {
        channel.receive(view) as InetSocketAddress? ?: return UDP_WOULD_BLOCK
    } catch (_: PortUnreachableException) {
        return UDP_CONN_ERROR
    } catch (e: IOException) {
        return -(1000 + JvmErrno.record(e))
    }
    val n = view.position()
    batch.lens[0] = n
    batch.strides[0] = n
    val address = from.address
    // A dual-stack socket reports an IPv4 peer as IPv4; native sockets as ::ffff:a.b.c.d (the address API's form).
    val bytes = if (Channels.isIpv6(fd)) ipv6Bytes(address) else address.address
    bytes.copyInto(batch.ips, 0)
    batch.families[0] = if (bytes.size == 16) 6 else 4
    batch.ports[0] = from.port
    batch.scopes[0] = (address as? Inet6Address)?.scopeId ?: 0
    batch.ecns[0] = 0
    batch.dstFamilies[0] = 0
    return 1
}

internal actual fun udpSend(fd: Int, sockV6: Boolean, t: Transmit, einvalMode: Boolean): Int {
    val channel = Channels[fd] as? DatagramChannel ?: return -(1000 + JvmErrno.EBADF)
    val to = InetSocketAddress(inetAddress(t.dstFamily, t.dstIp, 0, t.dstScope), t.dstPort)
    // No GSO: a segmented transmit (never made with maxGsoSegments 1) goes out datagram by datagram.
    val segment = if (t.segmentSize in 1 until t.length) t.segmentSize else t.length
    var at = 0
    while (at < t.length || at == 0) {
        val n = minOf(segment, t.length - at)
        val sent = try {
            channel.send(ByteBuffer.wrap(t.buffer, at, n), to)
        } catch (e: IOException) {
            val message = e.message.orEmpty().lowercase()
            if ("too long" in message || "too large" in message || "message size" in message) return UDP_MSG_SIZE
            if (e is PortUnreachableException) return t.length                       // reported later; the datagram left
            return -(1000 + JvmErrno.record(e))
        }
        if (sent == 0 && n > 0) return if (at == 0) UDP_WOULD_BLOCK else at       // the rest is lost, as on the wire
        at += n
        if (n == 0) break
    }
    return at
}

/** An InetAddress from the address fields (an IPv4-mapped IPv6 address stays IPv6, as the native layer passes it). */
private fun inetAddress(family: Int, ip: ByteArray, offset: Int, scope: Int): InetAddress =
    if (family == 4) {
        InetAddress.getByAddress(ip.copyOfRange(offset, offset + 4))
    } else {
        val bytes = ip.copyOfRange(offset, offset + 16)
        if (scope != 0) Inet6Address.getByAddress(null, bytes, scope) else InetAddress.getByAddress(bytes)
    }
