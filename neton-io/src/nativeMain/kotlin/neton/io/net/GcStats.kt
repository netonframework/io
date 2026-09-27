@file:OptIn(kotlin.native.runtime.NativeRuntimeApi::class, kotlin.ExperimentalStdlibApi::class, kotlinx.cinterop.ExperimentalForeignApi::class, kotlin.native.concurrent.ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.cinterop.toKString
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker

/**
 * Bench-side GC statistics for echoServer (SPEC §26.3), enabled by `NETON_IO_GC_STATS=1`.
 *
 * A sampler thread reads `GC.lastGCInfo` every 2 ms and prints one line per second:
 * collections (epoch difference, exact); for the collections sampled (only the last one between two
 * samples is seen) the stop-the-world time (both pauses, start to end) and the time to safepoint
 * (pause requested to pause started: mutators running on until they reach a safepoint), sum and
 * maximum; and the heap after the last collection.
 */
internal object GcStats {
    fun startFromEnvironment() {
        if (platform.posix.getenv("NETON_IO_GC_STATS")?.toKString() != "1") return
        Worker.start(name = "gc-stats").execute(TransferMode.SAFE, {}) { sample() }
    }

    private fun sample() {
        var firstEpoch = -1L
        var lastEpoch = -1L
        var sampled = 0L
        var pauseSumNs = 0L
        var pauseMaxNs = 0L
        var ttspSumNs = 0L
        var ttspMaxNs = 0L
        var heapAfter = 0L
        var tick = 0
        while (true) {
            platform.posix.usleep(2_000u)
            val info = kotlin.native.runtime.GC.lastGCInfo
            if (info != null && info.epoch != lastEpoch) {
                if (firstEpoch < 0) firstEpoch = info.epoch - 1
                lastEpoch = info.epoch
                val end1 = info.firstPauseEndTimeNs
                if (end1 != null) {
                    var pause = end1 - info.firstPauseStartTimeNs
                    var ttsp = info.firstPauseStartTimeNs - info.firstPauseRequestTimeNs
                    val start2 = info.secondPauseStartTimeNs
                    val end2 = info.secondPauseEndTimeNs
                    if (start2 != null && end2 != null) {
                        pause += end2 - start2
                        info.secondPauseRequestTimeNs?.let { ttsp += start2 - it }
                    }
                    sampled++
                    pauseSumNs += pause; if (pause > pauseMaxNs) pauseMaxNs = pause
                    ttspSumNs += ttsp; if (ttsp > ttspMaxNs) ttspMaxNs = ttsp
                }
                heapAfter = info.memoryUsageAfter["heap"]?.totalObjectsSizeBytes ?: heapAfter
            }
            if (++tick % 500 == 0) {
                val gcs = if (firstEpoch < 0) 0 else lastEpoch - firstEpoch
                println("gc-stats t=${tick / 500}s gcs=$gcs sampled=$sampled pause_sum_us=${pauseSumNs / 1000} pause_max_us=${pauseMaxNs / 1000} ttsp_sum_us=${ttspSumNs / 1000} ttsp_max_us=${ttspMaxNs / 1000} heap_after_kib=${heapAfter / 1024}")
            }
        }
    }
}
