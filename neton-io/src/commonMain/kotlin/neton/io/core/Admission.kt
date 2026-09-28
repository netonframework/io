@file:OptIn(kotlin.concurrent.atomics.ExperimentalAtomicApi::class)

package neton.io.core

import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.concurrent.atomics.AtomicLong
import kotlin.time.TimeSource

/**
 * Admission before reading (SPEC §28.12): at most [permits] requests are being read and handled at
 * once, across every connection (and service) sharing this instance.
 *
 * A connection holds no permit while it waits for the first byte of its next request, so idle
 * connections never take one. Once a byte of a request is buffered it takes a permit, then reads
 * the rest, decodes, calls the service and hands the response to the write path; the permit is
 * released right after that (or in `finally` on error, close or cancellation — exactly once).
 * While no permit is free the connection stops reading: bytes already buffered stay there, the
 * read buffer does not grow, the TCP receive window closes and the client is pushed back. Waiting
 * longer than [acquireTimeoutMillis] fails the connection with [AdmissionTimeoutException]
 * (counted in [timeouts]), so queueing under overload has a limit.
 *
 * Every connection waits for at most one permit, so waiters ≤ connections: the queue is bounded
 * only when the server sets `maxConnections` > 0. Accepting is not paused by exhausted permits;
 * `maxConnections` governs that separately. Use with a frame read rate (required by
 * [Framed.serveLoop] with admission): it bounds how long a permit holder may wait for the rest of
 * its request. Combines with [limitInFlight], which bounds calls being executed.
 */
class Admission(val permits: Int, val acquireTimeoutMillis: Long) {
    init { require(permits > 0 && acquireTimeoutMillis > 0) }

    private val sem = Semaphore(permits)
    private val timeoutCount = AtomicLong(0)

    /** Permits held now. */
    val inUse: Int get() = permits - sem.availablePermits

    /** Connections failed with [AdmissionTimeoutException] so far. */
    val timeouts: Long get() = timeoutCount.load()

    // Waits (contended acquires only; the uncontended path records nothing): count, and a log2
    // histogram of the wait in microseconds, bucket k = [2^k, 2^(k+1)) µs.
    private val waitCount = AtomicLong(0)
    private val waitBuckets = Array(WAIT_BUCKETS) { AtomicLong(0) }

    /** Acquires that had to wait for a permit (including those that timed out). */
    val waits: Long get() = waitCount.load()

    /** Upper bound of the [p]-quantile of permit waits, in microseconds (0 if nothing waited). */
    fun waitQuantileMicros(p: Double): Long {
        val total = waitCount.load(); if (total == 0L) return 0
        val want = kotlin.math.ceil(total * p).toLong().coerceAtLeast(1)
        var acc = 0L
        for (k in 0 until WAIT_BUCKETS) { acc += waitBuckets[k].load(); if (acc >= want) return 1L shl (k + 1) }
        return 1L shl WAIT_BUCKETS
    }

    /**
     * Take a permit, waiting at most [acquireTimeoutMillis] (then [AdmissionTimeoutException]). The uncontended path
     * allocates nothing. A caller that got a permit must [release] it exactly once (in `finally`); a cancelled or
     * timed-out acquire holds none. Protocol servers use this with the rules in the class notes.
     */
    suspend fun acquire() {
        if (sem.tryAcquire()) return
        val start = TimeSource.Monotonic.markNow()
        // A cancelled acquire (timeout or connection cancellation) does not keep a permit.
        val got = withTimeoutOrNull(acquireTimeoutMillis) { sem.acquire() }
        recordWait(start.elapsedNow().inWholeMicroseconds)
        if (got == null) {
            timeoutCount.addAndFetch(1L)
            throw AdmissionTimeoutException(acquireTimeoutMillis)
        }
    }

    private fun recordWait(us: Long) {
        waitCount.addAndFetch(1L)
        val k = (63 - us.coerceAtLeast(1).countLeadingZeroBits()).coerceAtMost(WAIT_BUCKETS - 1)
        waitBuckets[k].addAndFetch(1L)
    }

    /** Take a permit only if one is free now (no wait recorded). */
    fun tryAcquire(): Boolean = sem.tryAcquire()

    internal fun tryAcquireForLoop(): Boolean = tryAcquire()

    /** Give back a permit taken with [acquire] or [tryAcquire]. */
    fun release() = sem.release()
}

private const val WAIT_BUCKETS = 32

/** A connection waited longer than [Admission.acquireTimeoutMillis] for a permit and is closed. */
class AdmissionTimeoutException(waitedMillis: Long) : IoException("no admission permit within $waitedMillis ms")
