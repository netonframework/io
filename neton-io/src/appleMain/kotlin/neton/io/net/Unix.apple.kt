package neton.io.net

// Apple sockaddr_un: u8 sun_len, u8 sun_family, char sun_path[104].
internal actual val SUN_HAS_LEN: Boolean = true
internal actual val SUN_PATH_SIZE: Int = 104
internal actual val UNIX_ABSTRACT_SUPPORTED: Boolean = false
