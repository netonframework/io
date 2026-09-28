@file:OptIn(kotlin.native.runtime.NativeRuntimeApi::class, kotlin.ExperimentalStdlibApi::class)

package neton.io.net

import kotlin.native.runtime.GC
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * SPEC §26.8: [GcTuning.setMinHeap] keeps autotuning on, so with more than 9 MiB alive the GC still
 * collects rarely. Turning autotuning off with a large target (the old `fixTargetHeap`) leaves the
 * 2.4.0 runtime's trigger at 9 MiB: it then collects back to back. Both are measured and printed;
 * only the floor's own behaviour is asserted (a future runtime may fix the other).
 */
class GcTuningTest {
    private var sink: Any? = null

    /** Keep ~16 MiB alive, churn ~200 MiB of garbage; returns how many collections that took. */
    private fun collectionsFor(setup: () -> Unit): Long {
        GC.collect()
        setup()
        val live = Array(16) { ByteArray(1 shl 20) }
        val before = GC.lastGCInfo?.epoch ?: 0L
        repeat(200 * 16) { sink = ByteArray(64 * 1024) }
        GC.collect()
        val n = (GC.lastGCInfo?.epoch ?: 0L) - before - 1              // minus the explicit collect
        sink = live
        return n
    }

    @Test
    fun minHeapKeepsCollectionsRare() {
        val auto = GC.autotune; val target = GC.targetHeapBytes; val min = GC.minHeapBytes
        try {
            val off = collectionsFor { GC.autotune = false; GC.targetHeapBytes = 64L shl 20 }
            GC.autotune = true; GC.targetHeapBytes = target; GC.minHeapBytes = min
            val floor = collectionsFor { GcTuning.setMinHeap(64) }
            println("GcTuningTest: collections for ~200 MiB churn with 16 MiB alive: autotune off + target 64 MiB = $off, setMinHeap(64) = $floor")
            assertTrue(floor < 20, "setMinHeap(64) took $floor collections")
        } finally {
            GC.autotune = auto; GC.targetHeapBytes = target; GC.minHeapBytes = min
        }
    }

    @OptIn(kotlin.experimental.ExperimentalNativeApi::class)
    @Test
    fun lowerGcThreadPriorityRenicesTheGcThreadOnLinux() {
        kotlin.test.assertFailsWith<IllegalArgumentException> { GcTuning.lowerGcThreadPriority(0) }
        val changed = GcTuning.lowerGcThreadPriority(10)
        if (kotlin.native.Platform.osFamily == kotlin.native.OsFamily.LINUX) assertTrue(changed >= 1, "no GC thread reniced ($changed)")
        else kotlin.test.assertEquals(0, changed)
    }
}

