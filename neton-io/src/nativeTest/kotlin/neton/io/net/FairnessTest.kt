@file:OptIn(kotlin.native.concurrent.ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.serve
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.coroutines.Continuation
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.native.concurrent.Worker
import kotlin.time.TimeSource

// Test ports are deliberately below 32768, outside the kernel's ephemeral range
// (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999). A fixed listen port
// inside that range intermittently loses the bind to some other process's outbound
// connection, which SO_REUSEADDR does not help with — it surfaces as a flaky EADDRINUSE.
/**
 * Fairness: work that keeps re-dispatching itself must not starve I/O and timers. With an
 * unbounded task budget the loop would never reach the poller while a yield() loop is alive.
 */
class FairnessTest {

    @Test
    fun busyYieldLoopDoesNotStarveIoOrTimers() = runReactor {
        val port = 19795
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            while (true) {
                val conn = server.accept()
                launch { serve(Framed(Io(conn), LineCodec, LineCodec)) { it } }
            }
        }
        var spins = 0L
        val busy: Job = launch { while (true) { spins++; yield() } }

        // I/O must make progress next to the busy coroutine, within a bounded time.
        val client = connect("127.0.0.1", port)
        val framed = Framed(Io(client), LineCodec, LineCodec)
        val reply = withTimeout(5_000) {
            framed.send("ping")
            framed.incoming().first()
        }
        assertEquals("ping", reply)

        // Timers must fire too.
        withTimeout(5_000) { delay(20) }
        assertTrue(spins > 0)

        busy.cancelAndJoin()
        client.close()
        serverJob.cancelAndJoin()
        server.close()
    }

    /**
     * SPEC §28.4 F2: resume-ring entries that never run out (coroutine pairs handing off through
     * [ReactorResumer], more entries than the task budget) must not starve ordinary tasks: a newly
     * launched coroutine and a cross-thread resume still run. With ring-first priority neither ever did.
     */
    @Test
    fun resumeRingHandoffsDoNotStarveOrdinaryTasks() {
        val helper = Worker.start(name = "fairness-f2")
        runReactor {
            val resumer = reactorResumer(coroutineContext)!!
            class Pair { var parked: Continuation<Unit>? = null }
            var busy = true
            val pairs = List(512) { Pair() }
            val jobs = List(1024) { k ->
                val pair = pairs[k / 2]
                launch(start = CoroutineStart.UNDISPATCHED) {
                    while (busy) {
                        suspendCoroutineUninterceptedOrReturn<Unit> { c ->
                            val other = pair.parked
                            pair.parked = c
                            if (other != null) resumer.resume(other, Unit)
                            COROUTINE_SUSPENDED
                        }
                    }
                    pair.parked?.let { o -> pair.parked = null; resumer.resume(o, Unit) }
                }
            }
            var launched = false
            launch { launched = true }
            val cross = CompletableDeferred<Unit>()
            var crossRan = false
            launch(start = CoroutineStart.UNDISPATCHED) { cross.await(); crossRan = true }
            helper.executeAfter(0L) { cross.complete(Unit) }
            // delay() resumes from the timer path, which runs whatever the task queues hold.
            val t0 = TimeSource.Monotonic.markNow()
            while (!(launched && crossRan) && t0.elapsedNow().inWholeMilliseconds < 2_000) delay(10)
            val waited = t0.elapsedNow().inWholeMilliseconds
            // Read before the load stops: once it does, the starved tasks run and would hide the failure.
            val launchedInTime = launched; val crossInTime = crossRan
            busy = false
            jobs.forEach { it.join() }
            assertTrue(launchedInTime, "a launched coroutine never ran while the resume ring stayed busy")
            assertTrue(crossInTime, "a cross-thread resume never ran while the resume ring stayed busy")
            println("FairnessTest.resumeRingHandoffsDoNotStarveOrdinaryTasks: both ran within $waited ms")
        }
        helper.requestTermination().result
    }
}
