@file:OptIn(kotlin.native.concurrent.ObsoleteWorkersApi::class, kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.posixshim.neton_signal_disposition
import platform.posix.SIGINT
import platform.posix.SIGTERM
import platform.posix.raise
import kotlin.concurrent.AtomicInt
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * SPEC §27.1 / §27.7 in this process. Every raise() is preceded by a check that our handler is
 * installed for that signal, so a failing test cannot end the test process by default action; and
 * every test checks that the previous action is back afterwards.
 */
class SignalsTest {
    private suspend fun waitFor(ms: Long, cond: () -> Boolean) = withTimeout(ms) { while (!cond()) delay(5) }
    private fun disp(sig: Int) = neton_signal_disposition(sig)

    private fun safeRaise(sig: Int) {
        if (disp(sig) != 1) fail("refusing to raise signal $sig: our handler is not installed (disposition ${disp(sig)})")
        raise(sig)
    }

    @Test
    fun awaitSignalRestoresThePreviousActionAfterward() = runReactor {
        val before = disp(SIGTERM)
        val got = async { awaitSignal(Signal.Term) }
        waitFor(2_000) { disp(SIGTERM) == 1 }
        safeRaise(SIGTERM)
        assertEquals(Signal.Term, withTimeout(2_000) { got.await() })
        assertEquals(before, disp(SIGTERM), "the previous action must be back")
    }

    @Test
    fun cancelledWaiterRestoresThePreviousAction() = runReactor {
        val before = disp(SIGINT)
        val job = launch { awaitSignal(Signal.Int) }
        waitFor(2_000) { disp(SIGINT) == 1 }
        job.cancelAndJoin()
        assertEquals(before, disp(SIGINT))
    }

    /** serveTcp on its own thread; `done` becomes 1 when it returns. */
    private fun startServer(port: Int, signals: Boolean, until: CompletableDeferred<Unit>? = null): AtomicInt {
        val done = AtomicInt(0)
        Worker.start(name = "serve-$port").execute(TransferMode.SAFE, { Triple(port, done, Pair(signals, until)) }) { (p, d, o) ->
            serveTcp("127.0.0.1", p, reactors = 2, until = o.second, shutdownOnSignals = o.first, shutdownTimeoutMillis = 10_000) { conn ->
                try { val b = Buffer(); while (conn.read(b) >= 0) b.clear() } finally { conn.close() }
            }
            d.value = 1
        }
        return done
    }

    @Test
    fun serveTcpLeavesSignalsAloneByDefault() = runReactor {
        val before = disp(SIGTERM) to disp(SIGINT)
        val stop = CompletableDeferred<Unit>()
        val done = startServer(21952, signals = false, until = stop)
        withTimeout(3_000) { while (!tryConnectProbe(21952)) delay(20) }
        assertEquals(before, disp(SIGTERM) to disp(SIGINT), "serveTcp must not touch signals unless asked")
        stop.complete(Unit)
        waitFor(3_000) { done.value == 1 }
    }

    @Test
    fun serveTcpStopsGracefullyOnSigterm() = runReactor {
        val before = disp(SIGTERM)
        val done = startServer(21950, signals = true)
        waitFor(3_000) { disp(SIGTERM) == 1 }
        val c = connect("127.0.0.1", 21950)
        delay(100)
        safeRaise(SIGTERM)
        delay(500)
        assertEquals(0, done.value, "an open connection must hold a graceful shutdown")
        c.close()
        waitFor(5_000) { done.value == 1 }
        assertEquals(before, disp(SIGTERM))
    }

    @Test
    fun secondSignalForcesTheGracefulStop() = runReactor {
        val before = disp(SIGTERM)
        val done = startServer(21953, signals = true)
        waitFor(3_000) { disp(SIGTERM) == 1 }
        val c = connect("127.0.0.1", 21953)
        delay(100)
        safeRaise(SIGTERM)
        delay(300)
        assertEquals(0, done.value, "the first signal starts a graceful stop (10 s grace)")
        safeRaise(SIGTERM)                                                // still handled: forces it
        waitFor(3_000) { done.value == 1 }
        assertEquals(before, disp(SIGTERM))
        c.close()
    }

    /** Two signals back to back: the second must not be lost between the first and the force wait. */
    @Test
    fun backToBackSignalsForceTheStop() = runReactor {
        val before = disp(SIGTERM)
        val done = startServer(21954, signals = true)
        waitFor(3_000) { disp(SIGTERM) == 1 }
        val c = connect("127.0.0.1", 21954)
        delay(100)
        safeRaise(SIGTERM)
        safeRaise(SIGTERM)
        waitFor(3_000) { done.value == 1 }                                // not the 10 s grace period
        assertEquals(before, disp(SIGTERM))
        c.close()
    }

    @Test
    fun serveTcpStopsAtOnceOnSigint() = runReactor {
        val before = disp(SIGINT)
        val done = startServer(21951, signals = true)
        waitFor(3_000) { disp(SIGINT) == 1 }
        val c = connect("127.0.0.1", 21951)
        delay(100)
        safeRaise(SIGINT)
        waitFor(3_000) { done.value == 1 }                                // well before the 10 s grace period
        assertEquals(before, disp(SIGINT))
        c.close()
    }
}
