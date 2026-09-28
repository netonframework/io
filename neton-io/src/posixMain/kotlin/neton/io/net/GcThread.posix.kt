@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

internal actual fun gcThreadNice(nice: Int): Int = neton.io.posixshim.neton_gc_thread_nice(nice)
