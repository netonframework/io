@file:OptIn(kotlin.native.runtime.NativeRuntimeApi::class, kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString

/**
 * Process-wide GC tuning for servers (SPEC §24.6). Nothing here runs unless the application calls it.
 *
 * Kotlin/Native's GC pauses every thread, and its coordinator spins (`sched_yield`) until each mutator
 * reaches a safepoint, so each collection costs the reactor threads time. neton-io's own read/write
 * path allocates nothing, but protocol layers above it allocate per message (decoded objects,
 * payloads). Measured on 153: at 12 connections a fixed 64–256 MB target gave msgtrans rpc +5.5 % and
 * framed +4.6 %; at 1k and 10k connections (msgtrans SPEC §14) it gave 20× more collections and −11 to
 * −14 % throughput. The runtime's default (autotune) is the recommendation; measure before fixing a target.
 */
object GcTuning {
    /** Fix the GC target heap at [megabytes] and turn autotuning off. Call once, at startup. */
    fun fixTargetHeap(megabytes: Long) {
        require(megabytes > 0) { "megabytes must be positive" }
        kotlin.native.runtime.GC.autotune = false
        kotlin.native.runtime.GC.targetHeapBytes = megabytes shl 20
    }

    /** Apply `NETON_IO_GC_TARGET_MB` if it is set to a positive number; returns the value applied, or null. */
    fun fromEnvironment(): Long? {
        val mb = platform.posix.getenv("NETON_IO_GC_TARGET_MB")?.toKString()?.toLongOrNull() ?: return null
        if (mb <= 0) return null
        fixTargetHeap(mb)
        return mb
    }
}
