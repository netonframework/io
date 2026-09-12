package neton.io.net

import platform.posix.MSG_NOSIGNAL

/** Linux has no per-socket option; SIGPIPE is suppressed per call with MSG_NOSIGNAL. */
internal actual fun suppressSigpipe(fd: Int) {}

internal actual val SEND_FLAGS: Int = MSG_NOSIGNAL
