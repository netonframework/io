@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.core

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned

internal actual fun platformSecureRandom(dst: ByteArray, offset: Int, length: Int) {
    dst.usePinned { platform.posix.arc4random_buf(it.addressOf(offset), length.convert()) }
}
