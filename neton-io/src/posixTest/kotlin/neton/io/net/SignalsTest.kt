@file:OptIn(kotlin.native.concurrent.ObsoleteWorkersApi::class, kotlin.concurrent.atomics.ExperimentalAtomicApi::class)

package neton.io.net

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import platform.posix.SIGINT
import platform.posix.SIGTERM
import platform.posix.raise
import kotlin.concurrent.atomics.AtomicInt
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §27.1: signals reach `awaitSignal`; `serveTcp` stops gracefully on SIGTERM and at once on SIGINT. */
class SignalsTest {
    private suspend fun waitFor(ms: Long, cond: () -> Boolean) = withTimeout(ms) { while (!cond()) delay(5) }

    @Test
    fun awaitSignalReturnsTheSignal() = runReactor {
        val got = async { awaitSignal(Signal.Term) }
        waitFor(2_000) { signalWaiterCount() > 0 }
        raise(SIGTERM)
        assertEquals(Signal.Term, withTimeout(2_000) { got.await() })
    }

    /** Runs serveTcp on its own thread; returns (finished flag, worker). */
    private fun startServer(port: Int): AtomicInt {
        val done = AtomicInt(0)
        Worker.start(name = "serve-$port").execute(TransferMode.SAFE, { Pair(port, done) }) { (p, d) ->
            serveTcp("127.0.0.1", p, reactors = 2, shutdownTimeoutMillis = 10_000) { conn ->
                try { val b = Buffer(); while (conn.read(b) >= 0) b.clear() } finally { conn.close() }
            }
            d.store(1)
        }
        return done
    }

    @Test
    fun serveTcpStopsGracefullyOnSigterm() = runReactor {
        signalInstall(Signal.Term); signalInstall(Signal.Int)          // before any raise
        val before = signalWaiterCount()
        val done = startServer(21950)
        waitFor(3_000) { signalWaiterCount() > before }
        val c = connect("127.0.0.1", 21950)
        delay(100)
        raise(SIGTERM)
        delay(500)
        assertEquals(0, done.load(), "an open connection must hold a graceful shutdown")
        c.close()
        waitFor(5_000) { done.load() == 1 }
    }

    @Test
    fun serveTcpStopsAtOnceOnSigint() = runReactor {
        signalInstall(Signal.Term); signalInstall(Signal.Int)
        val before = signalWaiterCount()
        val done = startServer(21951)
        waitFor(3_000) { signalWaiterCount() > before }
        val c = connect("127.0.0.1", 21951)
        delay(100)
        raise(SIGINT)
        waitFor(3_000) { done.load() == 1 }                              // well before the 10 s grace period
        assertTrue(done.load() == 1)
        c.close()
    }
}
