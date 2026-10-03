package neton.io.core

actual fun monotonicNanos(): Long = System.nanoTime()

actual fun systemTimeMillis(): Long = System.currentTimeMillis()

/** The JDK's strong default source (on Linux and Android, the kernel's /dev/urandom pool). */
private val secureSource = java.security.SecureRandom()

internal actual fun platformSecureRandom(dst: ByteArray, offset: Int, length: Int) {
    val bytes = ByteArray(length)
    secureSource.nextBytes(bytes)
    bytes.copyInto(dst, offset)
}
