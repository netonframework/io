package neton.io.net

import kotlinx.coroutines.delay
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/**
 * SPEC §29.6: a 1 ms `delay` must wake about 1 ms later on every driver; QUIC's pacing and loss timers depend on it.
 * The bound is on the median, which a busy CI host does not move; the percentiles are printed for the record. A timer
 * tick of 15.6 ms (Windows' default) makes the median about 14 ms late.
 */
class TimerPrecisionTest {
    @Test
    fun oneMillisecondDelayWakesWithinAFewMilliseconds() = runReactor {
        val samples = 200
        val late = LongArray(samples)
        val clock = TimeSource.Monotonic
        for (i in 0 until samples) {
            val t0 = clock.markNow()
            delay(1)
            late[i] = t0.elapsedNow().inWholeMicroseconds - 1000
        }
        late.sort()
        val p50 = late[samples / 2]
        println("TimerPrecisionTest: driver=${currentReactor().driverNameForProbe()} delay(1) late p50 ${p50} us, " +
            "p99 ${late[samples * 99 / 100]} us, max ${late[samples - 1]} us, early ${late.count { it < 0 }}")
        assertTrue(p50 < 4000, "delay(1) woke a median $p50 us late")
    }
}
