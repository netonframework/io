package neton.io.net

import platform.posix.MSG_NOSIGNAL

/** Linux has no per-socket option; SIGPIPE is suppressed per call with MSG_NOSIGNAL. */
internal actual fun suppressSigpipe(fd: Int): Boolean = true

internal actual val SEND_FLAGS: Int = MSG_NOSIGNAL

// pthread_t differs between Linux and Android: read it through a fixed-width wrapper (posixshim).
@OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)
internal actual fun currentThreadId(): ULong = neton.io.posixshim.neton_thread_id()

internal actual val TCP_KEEP_IDLE_OPTION: Int = platform.posix.TCP_KEEPIDLE
internal actual val TCP_KEEP_INTERVAL_OPTION: Int = platform.posix.TCP_KEEPINTVL
internal actual val TCP_KEEP_COUNT_OPTION: Int = platform.posix.TCP_KEEPCNT

/** Linux/Android hash incoming connections across the SO_REUSEPORT sockets of a port. */
internal actual val reusePortBalancesLoad: Boolean = true

/** Linux has no per-fd switch; the reactor stops writing to its wake pipe before closing it. */
internal actual fun pipeNoSigpipe(fd: Int) {}

