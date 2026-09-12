package neton.io.net

/**
 * Byte-order helpers. `htons`/`htonl`/`inet_addr` are C macros/functions that cinterop
 * does not reliably expose, so we compute network byte order directly. All supported
 * targets are little-endian, but this is written to be endianness-correct regardless:
 * the returned value is laid out big-endian in memory when stored into the socket struct.
 */

/** Host-order [value] to network-order (big-endian) 16-bit. */
internal fun htons(value: UShort): UShort {
    val v = value.toInt() and 0xFFFF
    return (((v and 0xFF) shl 8) or ((v shr 8) and 0xFF)).toUShort()
}

/** Build a network-order IPv4 address (`in_addr.s_addr`) from dotted-quad [host]. */
internal fun ipv4NetworkOrder(host: String): UInt {
    if (host == "0.0.0.0") return 0u
    val parts = host.split(".")
    require(parts.size == 4) { "not an IPv4 literal: $host" }
    val a = parts[0].toInt() and 0xFF
    val b = parts[1].toInt() and 0xFF
    val c = parts[2].toInt() and 0xFF
    val d = parts[3].toInt() and 0xFF
    // network order = bytes [a][b][c][d]; on a little-endian host that is this uint32.
    return (a or (b shl 8) or (c shl 16) or (d shl 24)).toUInt()
}
