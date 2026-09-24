@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.convert
import platform.posix.poll
import platform.posix.pollfd

internal actual fun pollFds(fds: CPointer<pollfd>, count: Int, timeoutMillis: Int): Int =
    poll(fds, count.convert(), timeoutMillis)
