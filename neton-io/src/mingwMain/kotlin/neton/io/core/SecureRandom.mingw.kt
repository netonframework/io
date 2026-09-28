@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.core

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned

internal actual fun platformSecureRandom(dst: ByteArray, offset: Int, length: Int) {
    val rc = dst.usePinned { neton.io.win.neton_secure_random(it.addressOf(offset).reinterpret(), length.convert()) }
    check(rc == 0) { "BCryptGenRandom failed" }
}
