package neton.io

import neton.io.core.monotonicNanos
import neton.io.core.systemTimeMillis
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.TimeSource

class ClockTest {
    @Test
    fun monotonicAdvancesLikeTheStdlibClock() {
        val mark = TimeSource.Monotonic.markNow()
        val a = monotonicNanos()
        var x = 0L
        for (i in 0 until 2_000_000) x += i
        val b = monotonicNanos()
        val std = mark.elapsedNow().inWholeNanoseconds
        assertTrue(b >= a, "went backwards: $a -> $b ($x)")
        assertTrue(b - a <= std + 1_000_000, "monotonic ${b - a} ns vs stdlib $std ns")
    }

    @Test
    fun wallClockMatchesTheStdlibClock() {
        val std = kotlin.time.Clock.System.now().toEpochMilliseconds()
        assertTrue(abs(systemTimeMillis() - std) < 1_000, "wall clock ${systemTimeMillis()} vs $std")
    }
}
