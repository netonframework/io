@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

/**
 * The runtime starts its GC thread at startup, but on a busy or single core it may not have run (and named itself)
 * yet when the application's `main` calls this: wait for it, up to half a second, where renicing is supported.
 */
internal actual fun gcThreadNice(nice: Int): Int {
    var n = neton.io.posixshim.neton_gc_thread_nice(nice)
    if (!isLinuxFamily()) return n
    var waited = 0
    while (n == 0 && waited < 500) {
        platform.posix.usleep(1000u)
        waited++
        n = neton.io.posixshim.neton_gc_thread_nice(nice)
    }
    return n
}

@OptIn(kotlin.experimental.ExperimentalNativeApi::class)
private fun isLinuxFamily(): Boolean =
    kotlin.native.Platform.osFamily == kotlin.native.OsFamily.LINUX || kotlin.native.Platform.osFamily == kotlin.native.OsFamily.ANDROID
