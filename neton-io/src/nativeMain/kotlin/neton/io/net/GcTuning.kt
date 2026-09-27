@file:OptIn(kotlin.native.runtime.NativeRuntimeApi::class, kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString

/**
 * Process-wide GC tuning for servers (SPEC §24.6, §26.8). Nothing here runs unless the application calls it.
 *
 * Kotlin/Native's GC pauses every thread, and its coordinator waits until each mutator reaches a
 * safepoint, so each collection costs the reactor threads time. neton-io's own read/write path
 * allocates nothing, but protocol layers above it allocate per message; a larger heap floor makes
 * collections rarer.
 *
 * The runtime's autotuning stays on: after each collection it sets the next target to
 * `alive / targetHeapUtilization`, and [setMinHeap] only raises the floor of that target.
 * Turning autotuning off instead (the previous `fixTargetHeap`) looks equivalent but is not: with
 * autotuning off the 2.4.0 runtime never moves its trigger (`HeapGrowthController.updateBoundaries`),
 * which stays at 0.9 × the initial 10 MiB, so once more than 9 MiB is alive it collects back to
 * back (msgtrans SPEC §14.1: 20× the collections, −11 to −14 % throughput at 1k / 10k connections).
 */
object GcTuning {
    /** Never let the GC target heap fall below [megabytes] (autotuning stays on). Call once, at startup. */
    fun setMinHeap(megabytes: Long) {
        require(megabytes > 0) { "megabytes must be positive" }
        val bytes = megabytes shl 20
        kotlin.native.runtime.GC.autotune = true
        kotlin.native.runtime.GC.minHeapBytes = bytes
        // Takes effect for the target at once; the trigger follows after the first collection.
        if (kotlin.native.runtime.GC.targetHeapBytes < bytes) kotlin.native.runtime.GC.targetHeapBytes = bytes
    }

    /** Apply `NETON_IO_GC_MIN_HEAP_MB` if it is set to a positive number; returns the value applied, or null. */
    fun fromEnvironment(): Long? {
        val mb = platform.posix.getenv("NETON_IO_GC_MIN_HEAP_MB")?.toKString()?.toLongOrNull() ?: return null
        if (mb <= 0) return null
        setMinHeap(mb)
        return mb
    }
}
