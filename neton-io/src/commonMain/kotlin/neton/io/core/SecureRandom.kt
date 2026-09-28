package neton.io.core

/**
 * Fill [dst] (`[offset, offset + length)`) with cryptographically secure random bytes from the operating
 * system (SPEC §28.15): `arc4random_buf` on Apple, `/dev/urandom` on Linux and Android (one descriptor per
 * process; `getrandom` is newer than the Linux sysroot and Android API level these targets build against),
 * `BCryptGenRandom` on Windows. For hash-flooding keys, WebSocket masks and keys, QUIC connection IDs and tokens.
 * Throws [IllegalStateException] if the source fails, never returns predictable bytes.
 */
fun secureRandom(dst: ByteArray, offset: Int = 0, length: Int = dst.size - offset) {
    require(offset >= 0 && length >= 0 && offset <= dst.size && length <= dst.size - offset) { "invalid range" }
    if (length > 0) platformSecureRandom(dst, offset, length)
}

/** A secure random Long (see [secureRandom]). */
fun secureRandomLong(): Long {
    val b = ByteArray(8)
    secureRandom(b)
    var v = 0L
    for (x in b) v = (v shl 8) or (x.toLong() and 0xFF)
    return v
}

internal expect fun platformSecureRandom(dst: ByteArray, offset: Int, length: Int)
