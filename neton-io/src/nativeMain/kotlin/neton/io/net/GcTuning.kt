@file:OptIn(kotlin.native.runtime.NativeRuntimeApi::class, kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString

/**
 * Process-wide GC tuning for servers (SPEC §24.6). Nothing here runs unless the application calls it.
 *
 * Kotlin/Native's GC pauses every thread, and its coordinator spins (`sched_yield`) until each mutator
 * reaches a safepoint, so each collection costs the reactor threads time. neton-io's own read/write
 * path allocates nothing, but protocol layers above it allocate per message (decoded objects,
 * payloads); a larger, fixed GC target heap makes collections rarer. Measured on 153: msgtrans rpc
 * +5.5 % and framed +4.6 % throughput at a 64–256 MB target, with half the GC spin per request.
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
