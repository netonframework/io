@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.core

actual fun monotonicNanos(): Long = neton.io.win.neton_monotonic_nanos()

actual fun systemTimeMillis(): Long = neton.io.win.neton_realtime_millis()
