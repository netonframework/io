@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

actual fun cpuCount(): Int = neton.io.posixshim.neton_cpu_count()
