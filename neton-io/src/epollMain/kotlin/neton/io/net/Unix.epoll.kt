package neton.io.net

// Linux / Android sockaddr_un: u16 sun_family, char sun_path[108]; abstract names start with NUL.
internal actual val SUN_HAS_LEN: Boolean = false
internal actual val SUN_PATH_SIZE: Int = 108
internal actual val UNIX_ABSTRACT_SUPPORTED: Boolean = true
