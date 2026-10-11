package neton.io.net

import neton.io.core.ClosedException
import neton.io.core.IoException

// SPEC §29: the UDP datagram layer, after quinn-udp 0.11. Hot paths allocate nothing: batches and transmits own
// their arrays (on native pinned once for their lifetime, [RecvBatchPins] / [TransmitPins]); addresses on the receive
// path stay in primitive fields. The JVM (DatagramChannel, SPEC §37) receives one datagram per call and has no GSO,
// GRO, per-datagram ECN, source address choice or destination address.

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
    // Pinned once for the batch's lifetime on native: a pin per call would allocate on every receive.
    internal val pins = RecvBatchPins(this)
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
        pins.unpin()
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
    internal val pins = TransmitPins(this)
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
        pins.unpin()
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

/** Datagrams per receive call (Linux recvmmsg; 32 within the shim on Apple and Windows; 1 on the JVM), quinn-udp `BATCH_SIZE`. */
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

/** A batch's arrays pinned for its lifetime (native); nothing on the JVM. */
internal expect class RecvBatchPins(batch: RecvBatch) {
    fun unpin()
}

/** A transmit's arrays pinned for its lifetime (native); nothing on the JVM. */
internal expect class TransmitPins(transmit: Transmit) {
    fun unpin()
}

internal expect fun udpBatchSize(): Int
/** fd, or -errno. */
internal expect fun udpBind(family: Int, ip: ByteArray, port: Int, scope: Int, v6only: Boolean): Int
/** may_fragment (0/1) with caps[0] = GSO segments, caps[1] = GRO segments; or -errno. */
internal expect fun udpSetup(fd: Int, v6: Boolean, v6only: Boolean, caps: IntArray): Int
internal expect fun udpBuffer(fd: Int, which: Int, value: Int): Int
/** Datagrams received (> 0), or a UDP_* code (a negative errno for other failures, below -1000). */
internal expect fun udpRecv(fd: Int, batch: RecvBatch): Int
/** Bytes sent, or a UDP_* code. */
internal expect fun udpSend(fd: Int, sockV6: Boolean, t: Transmit, einvalMode: Boolean): Int
