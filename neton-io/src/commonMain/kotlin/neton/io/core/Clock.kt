package neton.io.core

/**
 * Monotonic time in nanoseconds since an arbitrary origin (SPEC §29.6). Never goes backwards; only differences are
 * meaningful. For protocol timers (QUIC loss detection, pacing) and measurements. A direct clock read that allocates
 * nothing (`clock_gettime(CLOCK_MONOTONIC)`, `QueryPerformanceCounter`).
 */
expect fun monotonicNanos(): Long

/** Wall-clock time in milliseconds since the Unix epoch (can jump when the system clock is set); allocates nothing. */
expect fun systemTimeMillis(): Long
