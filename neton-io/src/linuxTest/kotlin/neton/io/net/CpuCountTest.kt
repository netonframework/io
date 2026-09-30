@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import neton.io.posixshim.neton_thread_cpu_count
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CpuCountTest {
    @Test
    fun defaultReactorCountUsesCallingThreadAffinity() {
        val allowed = neton_thread_cpu_count()
        assertTrue(allowed > 0, "affinity must be available on the Linux test host")
        assertEquals(allowed, cpuCount())
    }
}
