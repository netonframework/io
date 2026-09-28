package neton.io.core

import kotlin.time.TimeSource

private val origin = TimeSource.Monotonic.markNow()

/**
 * Monotonic time in nanoseconds since an arbitrary process-wide origin (SPEC §29.6). Never goes backwards;
 * only differences are meaningful. For protocol timers (QUIC loss detection, pacing) and measurements.
 */
fun monotonicNanos(): Long = origin.elapsedNow().inWholeNanoseconds

/** Wall-clock time in milliseconds since the Unix epoch (can jump when the system clock is set). */
fun systemTimeMillis(): Long = kotlin.time.Clock.System.now().toEpochMilliseconds()
