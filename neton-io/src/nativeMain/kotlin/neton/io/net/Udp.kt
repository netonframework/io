@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.Pinned
import kotlinx.cinterop.pin
import neton.io.core.ClosedException
import neton.io.core.IoException

// SPEC §29: the UDP datagram layer, after quinn-udp 0.11. Hot paths allocate nothing: batches and transmits own
// their arrays, pinned once for their lifetime; addresses on the receive path stay in primitive fields.

/** An IP socket address: IPv4 or IPv6, port, IPv6 scope id. Immutable value. */
class SocketAddress private constructor(
    /** 4 or 6. */
    val family: Int,
    private val ip: ByteArray,
    val port: Int,
    val scopeId: Int,
) {
    init { require(family == 4 || family == 6 && ip.size == 16 && port in 0..65535) }

    val isIpv4: Boolean get() = family == 4
    val isIpv6: Boolean get() = family == 6

    /** The address bytes: 4 for IPv4, 16 for IPv6 (a copy). */
    fun ipBytes(): ByteArray = if (family == 4) ip.copyOf(4) else ip.copyOf()

    /** True for an IPv6 address of the form `::ffff:a.b.c.d`. */
    val isIpv4Mapped: Boolean get() = family == 6 && (0 until 10).all { ip[it] == 0.toByte() } && ip[10] == 0xff.toByte() && ip[11] == 0xff.toByte()

    /** `::ffff:a.b.c.d` for an IPv4 address; this address otherwise. */
    fun toIpv4Mapped(): SocketAddress =
        if (family == 6) this else ByteArray(16).also { ip.copyInto(it, 12, 0, 4); it[10] = -1; it[11] = -1 }.let { SocketAddress(6, it, port, 0) }

    /** The IPv4 address inside a v4-mapped IPv6 address; this address otherwise. */
    fun toCanonical(): SocketAddress = if (isIpv4Mapped) SocketAddress(4, ip.copyOfRange(12, 16).copyOf(16), port, 0) else this

    internal fun copyIpInto(dst: ByteArray, offset: Int = 0) { ip.copyInto(dst, offset, 0, 16) }

    /** Whether this address's IP equals the 16 bytes at [offset] of [bytes] (4 for IPv4). Allocation-free. */
    internal fun ipEquals(bytes: ByteArray, offset: Int): Boolean {
        for (i in 0 until (if (family == 4) 4 else 16)) if (bytes[offset + i] != ip[i]) return false
        return true
    }

    override fun equals(other: Any?): Boolean =
        other is SocketAddress && other.family == family && other.port == port && other.scopeId == scopeId &&
            (0 until (if (family == 4) 4 else 16)).all { other.ip[it] == ip[it] }

    override fun hashCode(): Int {
        var h = family * 31 + port
        for (i in 0 until (if (family == 4) 4 else 16)) h = h * 31 + ip[i]
        return h * 31 + scopeId
    }

    override fun toString(): String = if (family == 4) {
        "${ip[0].toInt() and 0xff}.${ip[1].toInt() and 0xff}.${ip[2].toInt() and 0xff}.${ip[3].toInt() and 0xff}:$port"
    } else {
        val groups = (0 until 8).joinToString(":") { (((ip[2 * it].toInt() and 0xff) shl 8) or (ip[2 * it + 1].toInt() and 0xff)).toString(16) }
        "[$groups${if (scopeId != 0) "%$scopeId" else ""}]:$port"
    }

    companion object {
        /** IPv4 from 4 bytes. */
        fun ipv4(a: Int, b: Int, c: Int, d: Int, port: Int) = SocketAddress(4, byteArrayOf(a.toByte(), b.toByte(), c.toByte(), d.toByte()).copyOf(16), port, 0)

        /** From raw bytes: 4 bytes (IPv4) or 16 bytes (IPv6). */
        fun of(ip: ByteArray, port: Int, scopeId: Int = 0): SocketAddress = when (ip.size) {
            4 -> SocketAddress(4, ip.copyOf(16), port, 0)
            16 -> SocketAddress(6, ip.copyOf(), port, scopeId)
            else -> throw IllegalArgumentException("an IP address has 4 or 16 bytes")
        }

        val IPV4_LOCALHOST_ANY_PORT: SocketAddress = ipv4(127, 0, 0, 1, 0)
        val IPV6_LOCALHOST_ANY_PORT: SocketAddress = of(ByteArray(16).also { it[15] = 1 }, 0)
        val IPV6_UNSPECIFIED_ANY_PORT: SocketAddress = of(ByteArray(16), 0)
        val IPV4_UNSPECIFIED_ANY_PORT: SocketAddress = of(ByteArray(4), 0)

        internal fun fromFields(family: Int, ip: ByteArray, offset: Int, port: Int, scope: Int): SocketAddress =
            SocketAddress(family, ip.copyOfRange(offset, offset + 16), port, if (family == 6) scope else 0)
    }
}

/** Explicit Congestion Notification codepoints (RFC 3168), as the low two bits of TOS / traffic class. */
enum class EcnCodepoint(val bits: Int) {
    Ect0(0b10), Ect1(0b01), Ce(0b11);

    companion object {
        fun fromBits(bits: Int): EcnCodepoint? = when (bits and 3) { 0b10 -> Ect0; 0b01 -> Ect1; 0b11 -> Ce; else -> null }
    }
}

/**
 * A reusable receive batch (quinn-udp `recv` with `IoSliceMut`s and `RecvMeta`s): [capacity] slots of [slotSize]
 * bytes and each datagram's metadata in primitive arrays. Slot `i` starts at `i * slotSize` in [buffer]. With GRO a
 * slot can hold several datagrams of [stride] bytes each (the last may be shorter). Pinned once; [close] releases it.
 */
class RecvBatch(val capacity: Int = BATCH_SIZE, val slotSize: Int = 65535) : AutoCloseable {
    init { require(capacity in 1..BATCH_SIZE && slotSize in 1..(1 shl 20)) }

    val buffer = ByteArray(capacity * slotSize)
    internal val lens = IntArray(capacity)
    internal val strides = IntArray(capacity)
    internal val families = IntArray(capacity)
    internal val ips = ByteArray(16 * capacity)
    internal val ports = IntArray(capacity)
    internal val scopes = IntArray(capacity)
    internal val ecns = IntArray(capacity)
    internal val dstFamilies = IntArray(capacity)
    internal val dstIps = ByteArray(16 * capacity)
    // Pinned once for the batch's lifetime: a pin per call would allocate on every receive.
    internal val pBuffer = buffer.pin(); internal val pLens = lens.pin(); internal val pStrides = strides.pin()
    internal val pFamilies = families.pin(); internal val pIps = ips.pin(); internal val pPorts = ports.pin()
    internal val pScopes = scopes.pin(); internal val pEcns = ecns.pin(); internal val pDstFamilies = dstFamilies.pin()
    internal val pDstIps = dstIps.pin()
    private var closed = false

    /** Bytes received in slot [i]. */
    fun length(i: Int): Int = lens[i]
    /** GRO segment size in slot [i] (equal to [length] without GRO). */
    fun stride(i: Int): Int = strides[i]
    /** Offset of slot [i] in [buffer]. */
    fun offset(i: Int): Int = i * slotSize
    /** Sender of slot [i] (allocates; see [sourcePort] / [sourceEquals] for allocation-free checks). */
    fun source(i: Int): SocketAddress = SocketAddress.fromFields(families[i], ips, 16 * i, ports[i], scopes[i])
    fun sourcePort(i: Int): Int = ports[i]
    /** Whether slot [i] came from [address], without allocating. */
    fun sourceEquals(i: Int, address: SocketAddress): Boolean =
        families[i] == address.family && ports[i] == address.port &&
            (address.family == 4 || scopes[i] == address.scopeId) && address.ipEquals(ips, 16 * i)
    fun ecn(i: Int): EcnCodepoint? = EcnCodepoint.fromBits(ecns[i])
    /** Local address the datagram was sent to (IP only, port 0), if the platform reported it. */
    fun destination(i: Int): SocketAddress? =
        if (dstFamilies[i] == 0) null else SocketAddress.fromFields(dstFamilies[i], dstIps, 16 * i, 0, 0)

    override fun close() {
        if (closed) return
        closed = true
        listOf<Pinned<*>>(pBuffer, pLens, pStrides, pFamilies, pIps, pPorts, pScopes, pEcns, pDstFamilies, pDstIps).forEach { it.unpin() }
    }
}

/**
 * A reusable transmit (quinn-udp `Transmit`): the datagram(s) in [buffer] `[0, length)`, destination, ECN,
 * optional GSO [segmentSize] (several datagrams of that size, the last may be shorter) and source IP. Pinned once;
 * [close] releases it.
 */
class Transmit(capacity: Int = 65535) : AutoCloseable {
    val buffer = ByteArray(capacity)
    var length: Int = 0
        set(v) { require(v in 0..buffer.size); field = v }
    var ecn: EcnCodepoint? = null
    /** GSO segment size, 0 for none (only used when smaller than [length], as in the reference). */
    var segmentSize: Int = 0
    internal var dstFamily = 0
    internal val dstIp = ByteArray(16)
    internal var dstPort = 0
    internal var dstScope = 0
    internal var srcFamily = 0
    internal val srcIp = ByteArray(16)
    internal val pBuffer = buffer.pin(); internal val pDstIp = dstIp.pin(); internal val pSrcIp = srcIp.pin()
    private var closed = false

    fun setDestination(address: SocketAddress) {
        dstFamily = address.family; address.copyIpInto(dstIp); dstPort = address.port; dstScope = address.scopeId
    }

    /** Source IP to send from (port ignored), or null for the kernel's choice. */
    fun setSource(address: SocketAddress?) {
        if (address == null) { srcFamily = 0; return }
        srcFamily = address.family; address.copyIpInto(srcIp)
    }

    override fun close() {
        if (closed) return
        closed = true
        pBuffer.unpin(); pDstIp.unpin(); pSrcIp.unpin()
    }
}

/** Options for [bindUdp]. */
class UdpOptions(
    /** For an IPv6 address: accept IPv6 only (false = dual-stack, v4 peers appear as v4-mapped). */
    val ipv6Only: Boolean = false,
    /** SO_SNDBUF / SO_RCVBUF, 0 = system default. */
    val sendBufferSize: Int = 0,
    val receiveBufferSize: Int = 0,
) {
    companion object { val Default = UdpOptions() }
}

/** Datagrams per receive call (Linux recvmmsg; 1 elsewhere), quinn-udp `BATCH_SIZE`. */
val BATCH_SIZE: Int get() = udpBatchSize()

/**
 * A UDP socket on the current reactor (SPEC §29.1). At most one [recv] and one [send] at a time; reactor thread only.
 */
class UdpSocket internal constructor(
    private val fd: Int,
    private val reactor: Reactor,
    val isIpv6: Boolean,
    val mayFragment: Boolean,
    gso: Int,
    val groSegments: Int,
) {
    /** Current GSO limit: drops to 1 at run time if the driver rejects segmentation (EIO / EINVAL). */
    var maxGsoSegments: Int = gso
        private set
    private var sendmsgEinval = false
    private var closed = false
    private var receiving = false
    private var sending = false

    val localAddress: SocketAddress
        get() {
            checkOpen()
            val f = IntArray(3); val ip = ByteArray(16)
            check(udpLocal(fd, f, ip) == 0) { "getsockname failed" }
            return SocketAddress.fromFields(f[0], ip, 0, f[1], f[2])
        }

    /** Receive up to [batch.capacity] datagrams; suspends until at least one arrives. Returns how many. */
    suspend fun recv(batch: RecvBatch): Int {
        check(!receiving) { "concurrent recv on a UDP socket" }
        receiving = true
        try {
            while (true) {
                checkOpen()
                val n = udpRecv(fd, batch)
                if (n > 0) return n
                when (n) {
                    UDP_WOULD_BLOCK -> reactor.awaitReadable(fd)
                    UDP_CONN_ERROR -> {}                 // ICMP unreachable reported on the socket: skip (SPEC §29.3)
                    else -> throw IoException("UDP receive failed (${-n})")
                }
            }
        } finally { receiving = false }
    }

    /** Send [transmit]; suspends while the send buffer is full. `EMSGSIZE` (MTU probes) counts as sent (SPEC §29.3). */
    suspend fun send(transmit: Transmit) {
        check(!sending) { "concurrent send on a UDP socket" }
        sending = true
        try {
            while (!trySendInternal(transmit)) reactor.awaitWritable(fd)
        } finally { sending = false }
    }

    /** Send without suspending; false if the send buffer is full. */
    fun trySend(transmit: Transmit): Boolean {
        check(!sending) { "concurrent send on a UDP socket" }
        return trySendInternal(transmit)
    }

    private fun trySendInternal(t: Transmit): Boolean {
        while (true) {
            checkOpen()
            val r = udpSend(fd, isIpv6, t, sendmsgEinval)
            if (r >= 0) return true
            when (r) {
                UDP_WOULD_BLOCK -> return false
                UDP_MSG_SIZE -> return true
                UDP_EIO, UDP_EINVAL -> {
                    // GSO unsupported by the driver: stop segmenting (quinn-udp unix.rs send).
                    if (t.segmentSize in 1 until t.length && maxGsoSegments > 1) maxGsoSegments = 1
                    if (r == UDP_EINVAL && !sendmsgEinval) { sendmsgEinval = true; continue }
                    throw IoException("UDP send failed (${if (r == UDP_EIO) "EIO" else "EINVAL"})")
                }
                else -> throw IoException("UDP send failed (${-r})")
            }
        }
    }

    fun sendBufferSize(): Int = udpBuffer(fd, 0, -1)
    fun receiveBufferSize(): Int = udpBuffer(fd, 1, -1)
    fun setSendBufferSize(bytes: Int) { require(bytes > 0); udpBuffer(fd, 0, bytes) }
    fun setReceiveBufferSize(bytes: Int) { require(bytes > 0); udpBuffer(fd, 1, bytes) }

    /** Close; a parked [recv] / [send] fails with [ClosedException]. Idempotent. */
    fun close() {
        if (closed) return
        closed = true
        reactor.closeStream(fd)
    }

    private fun checkOpen() { if (closed) throw ClosedException("UDP socket closed") }
}

/** Bind a UDP socket to [address] on the current reactor (SPEC §29.1). Port 0 picks a free port. */
suspend fun bindUdp(address: SocketAddress, options: UdpOptions = UdpOptions.Default): UdpSocket {
    val reactor = currentReactor()
    val ip = ByteArray(16).also { address.copyIpInto(it) }
    val fd = udpBind(address.family, ip, address.port, address.scopeId, options.ipv6Only)
    if (fd < 0) throw IoException("UDP bind to $address failed (errno ${-fd})")
    val caps = IntArray(2)
    val mayFragment = udpSetup(fd, address.isIpv6, options.ipv6Only, caps)
    if (mayFragment < 0) { closeFd(fd); throw IoException("UDP socket setup failed (errno ${-mayFragment})") }
    if (options.sendBufferSize > 0) udpBuffer(fd, 0, options.sendBufferSize)
    if (options.receiveBufferSize > 0) udpBuffer(fd, 1, options.receiveBufferSize)
    return UdpSocket(fd, reactor, address.isIpv6, mayFragment == 1, caps[0], caps[1])
}

// Result codes shared by the platform actuals (errno values differ per platform).
internal const val UDP_WOULD_BLOCK = -1
internal const val UDP_MSG_SIZE = -2
internal const val UDP_EIO = -3
internal const val UDP_EINVAL = -4
internal const val UDP_CONN_ERROR = -5

internal expect fun udpBatchSize(): Int
/** fd, or -errno. */
internal expect fun udpBind(family: Int, ip: ByteArray, port: Int, scope: Int, v6only: Boolean): Int
/** may_fragment (0/1) with caps[0] = GSO segments, caps[1] = GRO segments; or -errno. */
internal expect fun udpSetup(fd: Int, v6: Boolean, v6only: Boolean, caps: IntArray): Int
/** 0 with out[0] family, out[1] port, out[2] scope and [ip]; or -errno. */
internal expect fun udpLocal(fd: Int, out: IntArray, ip: ByteArray): Int

/** The peer ([peer] true, getpeername) or local address of socket [fd]; null for non-IP sockets or on failure. */
internal expect fun socketAddress(fd: Int, peer: Boolean): SocketAddress?
internal expect fun udpBuffer(fd: Int, which: Int, value: Int): Int
/** Datagrams received (> 0), or a UDP_* code (a negative errno for other failures, below -1000). */
internal expect fun udpRecv(fd: Int, batch: RecvBatch): Int
/** Bytes sent, or a UDP_* code. */
internal expect fun udpSend(fd: Int, sockV6: Boolean, t: Transmit, einvalMode: Boolean): Int
