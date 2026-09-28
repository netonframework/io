package neton.io.net

internal actual val connectionResetErrno: Int = platform.posix.ECONNRESET
