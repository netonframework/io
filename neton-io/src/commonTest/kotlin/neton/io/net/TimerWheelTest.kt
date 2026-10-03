package neton.io.net

import kotlin.test.Test
import kotlin.test.assertEquals

/** TimerWheel scan-position regressions (a deadline must never wait a full revolution). */
class TimerWheelTest {

    private class Node : WheelNode() {
        var fired = 0
        override fun onWheelDue(nowMs: Long): Long { fired++; return 0 }
    }

    /** First scan of a fresh wheel happens a tick after the deadline: it must still fire. */
    @Test
    fun firstScanPastTheDeadlineStillFires() {
        val wheel = TimerWheel()
        wheel.tick(1_000)                 // the reactor syncs the scan position before the first deadline
        val n = Node()
        wheel.schedule(n, 1_100)
        wheel.tick(1_125)                 // woke 2 ticks late
        assertEquals(1, n.fired)
        assertEquals(0, wheel.size)
    }

    /** Never ticked at all, first scan two ticks after the deadline. */
    @Test
    fun neverTickedWheelStillFires() {
        val wheel = TimerWheel()
        val n = Node()
        wheel.schedule(n, 1_100)
        wheel.tick(1_125)
        assertEquals(1, n.fired)
    }

    /** A deadline already behind the last scanned tick fires on the next tick. */
    @Test
    fun deadlineBehindTheScanPositionFiresNextTick() {
        val wheel = TimerWheel()
        val other = Node()
        wheel.schedule(other, 10_000)
        wheel.tick(2_000)                 // scan position now at tick 200
        val late = Node()
        wheel.schedule(late, 1_500)       // already past
        wheel.tick(2_010)
        assertEquals(1, late.fired)
        assertEquals(0, other.fired)
    }
}
