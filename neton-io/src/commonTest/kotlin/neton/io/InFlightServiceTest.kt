package neton.io

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import neton.io.core.Service
import neton.io.core.limitInFlight
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §27.3: `limitInFlight` bounds concurrent calls; cancelled waiters leak no permit. */
class InFlightServiceTest {
    @Test
    fun atMostMaxCallsRunAtOnce() = runBlocking {
        var running = 0; var peak = 0
        val svc = Service<Int, Int> { x -> running++; if (running > peak) peak = running; delay(20); running--; x * 2 }.limitInFlight(3)
        val results = (1..10).map { i -> async { svc.call(i) } }.awaitAll()
        assertEquals((1..10).map { it * 2 }, results)
        assertEquals(3, peak)
        assertEquals(0, svc.inFlight)
    }

    @Test
    fun cancelledWaiterLeaksNoPermit() = runBlocking {
        val gate = CompletableDeferred<Unit>()
        val svc = Service<Int, Int> { x -> gate.await(); x }.limitInFlight(1)
        val first = async { svc.call(1) }
        delay(20)
        val waiting = launch { svc.call(2) }                    // parked for the permit
        delay(20)
        waiting.cancel()
        gate.complete(Unit)
        assertEquals(1, first.await())
        assertEquals(3, withTimeout(1_000) { svc.call(3) })     // the permit came back
        assertTrue(svc.inFlight == 0)
    }
}
