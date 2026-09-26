package neton.io.net

import platform.posix.AF_UNIX
import platform.posix.EAGAIN
import platform.posix.ECONNREFUSED
import platform.posix.unlink

internal actual val AF_UNIX_FAMILY: Int = AF_UNIX
internal actual val CONNECT_REFUSED_CODE: Int = ECONNREFUSED
internal actual val CONNECT_RETRY_CODE: Int = EAGAIN
internal actual fun removeSocketFile(path: String): Boolean = unlink(path) == 0
