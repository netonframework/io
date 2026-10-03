package neton.io.net

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

    /**
     * The IP address alone as text, as Rust's `IpAddr` displays it: dotted IPv4; IPv6 in RFC 5952 form (lower-case hex,
     * the longest run of two or more zero groups, the first on a tie, as `::`), `::ffff:a.b.c.d` for a v4-mapped
     * address, and `%scope` when there is a scope id.
     */
    fun ipString(): String {
        if (family == 4) return "${ip[0].toInt() and 0xff}.${ip[1].toInt() and 0xff}.${ip[2].toInt() and 0xff}.${ip[3].toInt() and 0xff}"
        val scope = if (scopeId != 0) "%$scopeId" else ""
        if (isIpv4Mapped) return "::ffff:${ip[12].toInt() and 0xff}.${ip[13].toInt() and 0xff}.${ip[14].toInt() and 0xff}.${ip[15].toInt() and 0xff}$scope"
        val g = IntArray(8) { ((ip[2 * it].toInt() and 0xff) shl 8) or (ip[2 * it + 1].toInt() and 0xff) }
        var bestStart = -1; var bestLen = 0; var i = 0
        while (i < 8) {
            if (g[i] != 0) { i++; continue }
            val start = i
            while (i < 8 && g[i] == 0) i++
            if (i - start > bestLen) { bestStart = start; bestLen = i - start }
        }
        if (bestLen < 2) return g.joinToString(":") { it.toString(16) } + scope
        val head = (0 until bestStart).joinToString(":") { g[it].toString(16) }
        val tail = (bestStart + bestLen until 8).joinToString(":") { g[it].toString(16) }
        return "$head::$tail$scope"
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

/** 0 with out[0] family, out[1] port, out[2] scope and [ip]; or -errno. */
internal expect fun udpLocal(fd: Int, out: IntArray, ip: ByteArray): Int

/** The peer ([peer] true, getpeername) or local address of socket [fd]; null for non-IP sockets or on failure. */
internal expect fun socketAddress(fd: Int, peer: Boolean): SocketAddress?
