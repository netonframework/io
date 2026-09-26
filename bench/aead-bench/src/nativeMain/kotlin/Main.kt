@file:OptIn(dev.whyoleg.cryptography.DelicateCryptographyApi::class, kotlinx.cinterop.ExperimentalForeignApi::class, kotlin.ExperimentalUnsignedTypes::class)

import dev.whyoleg.cryptography.CryptographyProvider
import dev.whyoleg.cryptography.algorithms.AES
import dev.whyoleg.cryptography.providers.openssl3.Openssl3
import evp.*
import kotlinx.cinterop.*
import kotlin.time.TimeSource

// AES-128-GCM seal of one TLS-style record: explicit 12-byte nonce, 5-byte AAD, 16-byte tag.
// A: cryptography-kotlin 0.6.0 (OpenSSL provider): encryptWithIvBlocking(iv, plaintext, aad).
// B: direct libcrypto, one EVP_CIPHER_CTX per connection (key schedule once), per record only the
//    nonce is set, output written into a reused buffer.

private val key = ByteArray(16) { it.toByte() }
private val aad = byteArrayOf(23, 3, 3, 0, 0)

private fun direct(size: Int, n: Int): Double = memScoped {
    val ctx = EVP_CIPHER_CTX_new()!!
    check(EVP_EncryptInit_ex(ctx, EVP_aes_128_gcm(), null, key.toUByteArray().refTo(0), null) == 1)
    val pt = ByteArray(size) { 1 }; val out = ByteArray(size + 16); val iv = ByteArray(12)
    val outl = alloc<IntVar>()
    val t = TimeSource.Monotonic.markNow()
    repeat(n) { i ->
        iv[11] = i.toByte(); iv[10] = (i shr 8).toByte()
        iv.usePinned { ivp -> pt.usePinned { ptp -> out.usePinned { op -> aad.usePinned { ap ->
            EVP_EncryptInit_ex(ctx, null, null, null, ivp.addressOf(0).reinterpret())
            EVP_EncryptUpdate(ctx, null, outl.ptr, ap.addressOf(0).reinterpret(), aad.size)
            EVP_EncryptUpdate(ctx, op.addressOf(0).reinterpret(), outl.ptr, ptp.addressOf(0).reinterpret(), size)
            EVP_EncryptFinal_ex(ctx, op.addressOf(outl.value).reinterpret(), outl.ptr)
            EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, op.addressOf(size))
        } } } }
    }
    val ns = t.elapsedNow().inWholeNanoseconds.toDouble() / n
    EVP_CIPHER_CTX_free(ctx)
    ns
}

private fun library(size: Int, n: Int): Double {
    val aes = CryptographyProvider.Openssl3.get(AES.GCM)
    val k = aes.keyDecoder().decodeFromByteArrayBlocking(AES.Key.Format.RAW, key)
    val cipher = k.cipher()
    val pt = ByteArray(size) { 1 }; val iv = ByteArray(12)
    var sink = 0
    val t = TimeSource.Monotonic.markNow()
    repeat(n) { i ->
        iv[11] = i.toByte(); iv[10] = (i shr 8).toByte()
        sink += cipher.encryptWithIvBlocking(iv, pt, aad)[0]
    }
    val ns = t.elapsedNow().inWholeNanoseconds.toDouble() / n
    if (sink == 42) println()
    return ns
}

fun main() {
    // Same output for the same nonce: both paths compute the same thing.
    val a = run {
        val c = CryptographyProvider.Openssl3.get(AES.GCM).keyDecoder().decodeFromByteArrayBlocking(AES.Key.Format.RAW, key).cipher()
        c.encryptWithIvBlocking(ByteArray(12), ByteArray(64) { 1 }, aad)
    }
    println("check: library output ${a.size} bytes (ciphertext||tag)")
    for (size in intArrayOf(128, 1024, 16384)) {
        val n = if (size <= 1024) 400_000 else 60_000
        direct(size, n / 10); library(size, n / 10)            // warm-up
        val d = List(5) { direct(size, n) }.sorted()[2]
        val l = List(5) { library(size, n) }.sorted()[2]
        println("size=%6d  direct=%8.0f ns/record  cryptography-kotlin=%8.0f ns/record  ratio=%.2fx  (direct %.2f GB/s)".replace("%6d", size.toString()).let {
            "size=${size.toString().padStart(6)}  direct=${d.toInt().toString().padStart(6)} ns/record  cryptography-kotlin=${l.toInt().toString().padStart(6)} ns/record  ratio=${((l / d) * 100).toInt() / 100.0}x  direct ${(size / d * 100).toInt() / 100.0} GB/s"
        })
    }
}
