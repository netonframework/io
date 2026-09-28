package neton.io.net

import kotlinx.coroutines.delay
import kotlin.time.TimeSource

/**
 * SPEC §29.6 timer precision audit: how late `delay(d)` resumes on this reactor.
 * Usage: timerProbe [samples=2000] [delays=1,5]
 * Prints, per delay, lateness p50 / p99 / max in microseconds (lateness = elapsed - d).
 */
fun timerProbeMain(args: Array<String>) {
    val samples = args.getOrNull(0)?.toIntOrNull() ?: 2000
    val delays = (args.getOrNull(1) ?: "1,5").split(',').map { it.toLong() }
    val clock = TimeSource.Monotonic
    runReactor {
        println("timer-probe driver=${currentReactor().driverNameForProbe()} samples=$samples")
        for (d in delays) {
            val late = LongArray(samples)
            for (i in 0 until samples) {
                val t0 = clock.markNow()
                delay(d)
                late[i] = t0.elapsedNow().inWholeMicroseconds - d * 1000
            }
            late.sort()
            println("delay_ms=$d late_p50_us=${late[samples / 2]} late_p99_us=${late[samples * 99 / 100]} late_max_us=${late[samples - 1]} early=${late.count { it < 0 }}")
        }
    }
}
