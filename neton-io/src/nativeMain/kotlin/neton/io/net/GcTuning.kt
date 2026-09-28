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

    /**
     * Apply `NETON_IO_GC_MIN_HEAP_MB` if it is set to a positive number, and `NETON_IO_GC_THREAD_NICE` (see
     * [lowerGcThreadPriority]) if set; returns the heap floor applied, or null.
     */
    fun fromEnvironment(): Long? {
        platform.posix.getenv("NETON_IO_GC_THREAD_NICE")?.toKString()?.toIntOrNull()?.let { lowerGcThreadPriority(it) }
        val mb = platform.posix.getenv("NETON_IO_GC_MIN_HEAP_MB")?.toKString()?.toLongOrNull() ?: return null
        if (mb <= 0) return null
        setMinHeap(mb)
        return mb
    }

    /**
     * Give the runtime's GC thread a lower scheduling priority ([nice], 1..19; Linux / Android). Returns how many
     * threads were changed (0 where unsupported).
     *
     * Why (http SPEC §11, measured): the GC coordinator waits for every thread to reach a safepoint by spinning in
     * `sched_yield`. When a reactor shares its core with the GC thread, the scheduler keeps the spinning thread on the
     * core for a whole time slice (≈6 ms) while the reactor — the thread it waits for — cannot run: every collection
     * costs 6 ms of latency although the pause itself is ~15 µs. At a lower priority the reactor gets the core back at
     * once (time to safepoint 1 µs; hello-world p99 7.0 → 0.94 ms on one core). The GC still keeps up: 60 s at full
     * load on one core, heap flat, 23–26 collections/s. Call once at startup, after the runtime started its GC thread.
     */
    fun lowerGcThreadPriority(nice: Int = 19): Int {
        require(nice in 1..19) { "nice must be in 1..19" }
        return gcThreadNice(nice)
    }
}

/** Renice the runtime's GC threads; how many changed, 0 where unsupported, -1 on error. */
internal expect fun gcThreadNice(nice: Int): Int
