package neton.io.net

internal actual fun isConnectionResetErrno(errno: Int): Boolean = errno == platform.posix.ECONNRESET
