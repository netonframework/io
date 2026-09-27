@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.coroutines.launch
import neton.io.posixshim.neton_thread_cpu_count
import kotlin.concurrent.AtomicInt
import kotlin.test.Test
import kotlin.test.assertEquals

/** SPEC §27.2: with pinThreads every reactor thread may run on exactly one CPU (Linux / Android). */
class AffinityTest {
    @Test
    fun pinnedReactorsRunOnOneCpuEach() = runReactor {
        val g = listenGroup("127.0.0.1", 21955, reactors = 2, pinThreads = true)
        val counts = IntArray(2)
        counts[0] = neton_thread_cpu_count()
        val seen = AtomicInt(-2)
        g.callOnForTest(1) { seen.value = neton_thread_cpu_count() }
        counts[1] = seen.value
        if (counts[0] == -1) { println("SKIP affinity: not supported on this platform"); g.shutdown(0); return@runReactor }
        assertEquals(1, counts[0], "reactor 0")
        assertEquals(1, counts[1], "reactor 1")
        g.shutdown(0)
        g.awaitWorkers()
    }
}
