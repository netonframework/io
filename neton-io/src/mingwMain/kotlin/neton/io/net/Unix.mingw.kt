package neton.io.net

import platform.posix.WSAECONNREFUSED
import platform.posix.remove

// Windows 10 1803+ AF_UNIX (afunix.h): u16 sun_family, char sun_path[108]; no abstract namespace.
internal actual val AF_UNIX_FAMILY: Int = 1
internal actual val SUN_HAS_LEN: Boolean = false
internal actual val SUN_PATH_SIZE: Int = 108
internal actual val UNIX_ABSTRACT_SUPPORTED: Boolean = false
internal actual val CONNECT_REFUSED_CODE: Int = WSAECONNREFUSED
internal actual val CONNECT_RETRY_CODE: Int = -1   // Windows reports a full backlog as a refusal
internal actual fun removeSocketFile(path: String): Boolean = remove(path) == 0
