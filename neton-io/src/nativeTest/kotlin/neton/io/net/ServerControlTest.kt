package neton.io.net

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import kotlin.concurrent.AtomicInt
import kotlin.concurrent.AtomicReference
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/** SPEC §23.4: connection cap, pause / resume, graceful shutdown, SO_REUSEPORT accept mode. */
@OptIn(ObsoleteWorkersApi::class)
class ServerControlTest {

    /** Handler: count the connection, then hold it until the peer closes. */
    private fun holding(started: AtomicInt, cancelled: AtomicInt? = null): suspend (IoStream) -> Unit = { conn ->
        started.incrementAndGet()
        try {
            val b = Buffer()
            while (conn.read(b) >= 0) b.clear()
        } catch (e: CancellationException) {
            cancelled?.incrementAndGet(); throw e
        } finally {
            conn.close()
        }
    }

    private suspend fun waitFor(ms: Long, cond: () -> Boolean) = withTimeout(ms) { while (!cond()) delay(10) }

    @Test
    fun connectionCapWaitsForAFreeSlot() = runReactor {
        val started = AtomicInt(0)
        val g = listenGroup("127.0.0.1", 21910, reactors = 2, maxConnections = 2)
        val serveJob = launch { g.serve(holding(started)) }
        val c1 = connect("127.0.0.1", 21910); val c2 = connect("127.0.0.1", 21910); val c3 = connect("127.0.0.1", 21910)
        waitFor(2_000) { started.value == 2 }
        delay(300)
        assertEquals(2, started.value, "the third connection must wait for a slot")
        c1.close()
        waitFor(2_000) { started.value == 3 }
        c2.close(); c3.close()
        g.shutdown(1_000); serveJob.join(); g.awaitWorkers()
    }

    @Test
    fun pauseHoldsNewConnectionsUntilResume() = runReactor {
        val started = AtomicInt(0)
        val g = listenGroup("127.0.0.1", 21911, reactors = 2)
        val serveJob = launch { g.serve(holding(started)) }
        g.pause()
        val c = connect("127.0.0.1", 21911)
        delay(300)
        assertEquals(0, started.value, "no handler may run while paused")
        g.resume()
        waitFor(2_000) { started.value == 1 }
        c.close()
        g.shutdown(1_000); serveJob.join(); g.awaitWorkers()
    }

    @Test
    fun shutdownLetsConnectionsFinishThenCancelsTheRest() = runReactor {
        val started = AtomicInt(0); val cancelled = AtomicInt(0)
        val g = listenGroup("127.0.0.1", 21912, reactors = 2)
        val serveJob = launch { g.serve(holding(started, cancelled)) }
        val quick = connect("127.0.0.1", 21912)
        val stuck = connect("127.0.0.1", 21912)
        waitFor(2_000) { started.value == 2 }
        launch { delay(50); quick.close() }                  // ends on its own within the grace period
        val t = TimeSource.Monotonic.markNow()
        g.shutdown(300)
        val ms = t.elapsedNow().inWholeMilliseconds
        serveJob.join(); g.awaitWorkers()
        assertEquals(0, g.activeConnections)
        assertEquals(1, cancelled.value, "only the connection still open after the grace period is cancelled")
        assertTrue(ms in 250..3_000, "shutdown took $ms ms for a 300 ms grace period")
        stuck.close()
    }

    @Test
    fun reusePortModeSpreadsConnections() = runReactor {
        val threads = AtomicReference<Set<Int>>(emptySet())
        val g = listenGroup("127.0.0.1", 21913, reactors = 2, acceptMode = AcceptMode.ReusePort)
        val serveJob = launch {
            g.serve { conn ->
                val id = Worker.current.id
                while (true) { val cur = threads.value; if (threads.compareAndSet(cur, cur + id)) break }
                try {
                    val f = Framed(Io(conn), LineCodec, LineCodec)
                    f.incoming().collect { f.send(it) }
                } finally { conn.close() }
            }
        }
        val clients = (0 until 32).map { i -> launch {
            val c = connect("127.0.0.1", 21913)
            val f = Framed(Io(c), LineCodec, LineCodec)
            f.send("m$i")
            assertEquals("m$i", f.incoming().first())
            c.close()
        } }
        clients.forEach { it.join() }
        if (g.acceptMode == AcceptMode.ReusePort) {
            assertTrue(threads.value.size >= 2, "SO_REUSEPORT must spread 32 connections over both reactors, saw ${threads.value}")
        } else {
            println("SKIP reusePort spread: this platform falls back to ${g.acceptMode}")
        }
        g.shutdown(1_000); serveJob.join(); g.awaitWorkers()
    }
}
