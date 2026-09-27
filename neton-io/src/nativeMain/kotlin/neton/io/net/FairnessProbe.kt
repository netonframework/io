@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class, kotlin.native.concurrent.ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.concurrent.AtomicInt
import kotlin.concurrent.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.time.TimeSource

/**
 * SPEC §28.4 F2: the resume rings kept busy, and what the rest of the reactor gets meanwhile.
 *
 * Usage: fairnessProbe [pairs=1024] [samples=10000] [maxSeconds=120] [port=19400]
 *
 * `pairs` coroutine pairs hand a token back and forth through [ReactorResumer] (the path an I/O
 * completion takes), so every round's task budget is spent on resume-ring entries. At the same
 * time, on the same reactor, three things are measured until each has `samples` samples (or
 * `maxSeconds` pass):
 * - timer lateness: 10 coroutines looping `delay(10)`, lateness = elapsed - 10 ms;
 * - cross-thread dispatch wait: another thread completes a `CompletableDeferred` the reactor awaits,
 *   wait = complete() → the awaiting code runs;
 * - accept → handler: a client reactor on another thread connects repeatedly; accept returning →
 *   the launched handler's first line.
 * pairs=0 gives the floor of the same measurements without the load.
 * Prints one line per measurement (count, p50 / p99 / max in µs, plus how many were still waiting
 * at the end) and the reactor's NETON_IO_STATS line when NETON_IO_STATS=1.
 */
fun fairnessProbeMain(args: Array<String>) {
    val pairs = args.getOrNull(0)?.toIntOrNull() ?: 1024
    val want = args.getOrNull(1)?.toIntOrNull() ?: 10_000
    val maxSeconds = args.getOrNull(2)?.toIntOrNull() ?: 120
    val port = args.getOrNull(3)?.toIntOrNull() ?: 19400
    val clock = TimeSource.Monotonic.markNow()
    fun nowUs(): Long = clock.elapsedNow().inWholeMicroseconds

    val timerLate = LongArray(want); var timerN = 0
    val crossWait = LongArray(want); var crossN = 0
    val acceptWait = LongArray(want); var acceptN = 0
    val stop = AtomicInt(0)
    val crossSlot = AtomicReference<CompletableDeferred<Long>?>(null)
    val poster = Worker.start(name = "probe-poster")
    val connector = Worker.start(name = "probe-connector")
    var crossPendingSince = -1L
    var acceptPending = 0

    runReactor {
        val resumer = reactorResumer(coroutineContext)!!
        println("fairness-probe pairs=$pairs samples=$want driver=${currentReactor().driverNameForProbe()}")

        // Timers.
        val timerJobs = List(10) { i ->
            launch(start = CoroutineStart.UNDISPATCHED) {
                delay(i.toLong())
                while (timerN < want) {
                    val t0 = nowUs()
                    delay(10)
                    val late = nowUs() - t0 - 10_000
                    if (timerN < want) timerLate[timerN++] = late
                }
            }
        }

        // Cross-thread dispatch.
        poster.execute(TransferMode.SAFE, { Triple(crossSlot, stop, clock) }) { (slot, st, ck) ->
            var seed = 12345L
            while (st.value == 0) {
                val d = slot.value
                if (d != null && slot.compareAndSet(d, null)) d.complete(ck.elapsedNow().inWholeMicroseconds)
                seed = seed * 6364136223846793005L + 1442695040888963407L
                platform.posix.usleep((((seed ushr 33) % 1000) + 1).toUInt())
            }
        }
        val crossJob = launch(start = CoroutineStart.UNDISPATCHED) {
            while (crossN < want) {
                val d = CompletableDeferred<Long>()
                crossSlot.value = d
                crossPendingSince = nowUs()
                val sent = d.await()
                crossWait[crossN++] = nowUs() - sent
                crossPendingSince = -1L
            }
        }

        // Accept → handler.
        val listener = listen("127.0.0.1", port)
        connector.execute(TransferMode.SAFE, { Pair3(stop, port) }) { (st, pt) ->
            runReactor {
                while (st.value == 0) {
                    try { connect("127.0.0.1", pt).close() } catch (_: Throwable) { }
                    delay(1)
                }
            }
        }
        val acceptJob = launch(start = CoroutineStart.UNDISPATCHED) {
            try {
                while (acceptN < want) {
                    val conn = listener.accept()
                    val t0 = nowUs()
                    acceptPending++
                    launch {
                        acceptPending--
                        if (acceptN < want) acceptWait[acceptN++] = nowUs() - t0
                        conn.close()
                    }
                }
            } catch (_: neton.io.core.ClosedException) { }
        }

        // The load: each pair keeps exactly one entry in a resume ring at all times.
        class Pair { var parked: Continuation<Unit>? = null }
        var busy = true
        val pairList = List(pairs) { Pair() }
        val busyJobs = List(pairs * 2) { k ->
            val pair = pairList[k / 2]
            // Started in place: as ordinary tasks, the first pair's ring entries would run ahead of
            // the other launches forever (ring priority), leaving one entry instead of `pairs`.
            launch(start = CoroutineStart.UNDISPATCHED) {
                while (busy) {
                    suspendCoroutineUninterceptedOrReturn<Unit> { c ->
                        // First to arrive waits; later, whoever runs wakes the parked one and parks itself.
                        val other = pair.parked
                        pair.parked = c
                        if (other != null) resumer.resume(other, Unit)
                        COROUTINE_SUSPENDED
                    }
                }
                pair.parked?.let { o -> pair.parked = null; resumer.resume(o, Unit) }
            }
        }

        val started = nowUs()
        while ((timerN < want || crossN < want || acceptN < want) && nowUs() - started < maxSeconds * 1_000_000L) delay(100)
        val elapsed = (nowUs() - started) / 1e6
        stop.value = 1
        val crossStuck = if (crossPendingSince >= 0) nowUs() - crossPendingSince else -1L
        report("timer_late", timerLate, timerN, 0, -1)
        report("cross_thread_wait", crossWait, crossN, if (crossPendingSince >= 0) 1 else 0, crossStuck)
        report("accept_to_handler", acceptWait, acceptN, acceptPending, -1)
        println("elapsed_ms ${(elapsed * 1000).toLong()}")
        busy = false
        crossSlot.value?.complete(nowUs())
        listener.close()
        acceptJob.cancelAndJoin(); crossJob.cancelAndJoin(); timerJobs.forEach { it.cancelAndJoin() }
        while (busyJobs.any { it.isActive }) yield()
        dumpReactorStats()
    }
    poster.requestTermination().result
    connector.requestTermination().result
}

private data class Pair3(val stop: AtomicInt, val port: Int)

private fun report(name: String, a: LongArray, n: Int, pending: Int, pendingAgeUs: Long) {
    val s = a.copyOf(n); s.sort()
    fun pct(p: Double): Long = if (n == 0) -1 else s[((n * p).toInt()).coerceAtMost(n - 1)]
    val stuck = if (pendingAgeUs >= 0) " oldest_pending_us=$pendingAgeUs" else ""
    println("$name n=$n p50_us=${pct(0.50)} p99_us=${pct(0.99)} max_us=${if (n == 0) -1 else s[n - 1]} pending=$pending$stuck")
}

internal fun Reactor.driverNameForProbe(): String = this::class.simpleName ?: "?"
