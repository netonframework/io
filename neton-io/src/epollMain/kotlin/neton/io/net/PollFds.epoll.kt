@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import platform.posix.pollfd

// nfds_t differs between Linux and Android: the call goes through a fixed-width wrapper (posixshim).
internal actual fun pollFds(fds: CPointer<pollfd>, count: Int, timeoutMillis: Int): Int =
    neton.io.posixshim.neton_poll(fds, count, timeoutMillis)
