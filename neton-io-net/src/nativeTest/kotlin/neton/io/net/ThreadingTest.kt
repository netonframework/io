package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/** Threading contract: cross-thread resume is supported and wakes the loop; I/O off-thread is rejected; timers stay on the reactor. */
class ThreadingTest {

    @Test
    fun resumeFromAnotherThreadWakesTheReactor() = runReactor {
        val owner = currentThreadId()
        val gate = CompletableDeferred<String>()
        val worker = Worker.start()
        worker.execute(TransferMode.SAFE, { gate }) { g ->
            platform.posix.usleep(50_000u)
            g.complete("from-worker") // resumes the awaiter: dispatch() from a foreign thread
        }
        val t0 = TimeSource.Monotonic.markNow()
        val v = gate.await() // the loop is parked in the poller; the self-pipe must wake it
        assertEquals("from-worker", v)
        assertEquals(owner, currentThreadId(), "continuation must run on the reactor thread")
        assertTrue(t0.elapsedNow().inWholeMilliseconds < 5_000, "reactor was not woken promptly")
        worker.requestTermination().result
    }

    @Test
    fun delayAndWithTimeoutRunOnTheReactorThread() = runReactor {
        val owner = currentThreadId()
        val t0 = TimeSource.Monotonic.markNow()
        delay(30)
        val e = t0.elapsedNow().inWholeMilliseconds
        assertTrue(e in 25..2_000, "delay(30) took ${e}ms")
        assertEquals(owner, currentThreadId())

        val port = 39790
        val server = listen("127.0.0.1", port)
        var serverConn: IoStream? = null
        val accepted = launch { serverConn = server.accept() }
        val client = connect("127.0.0.1", port)
        accepted.join()
        // Nobody writes: the read must be timed out by the reactor's own timer, on this thread.
        assertFailsWith<TimeoutCancellationException> { withTimeout(50) { client.read(Buffer(64)) } }
        assertEquals(owner, currentThreadId())
        client.close(); serverConn!!.close(); server.close()
    }

    @Test
    fun ioFromAnotherThreadIsRejected() = runReactor {
        val port = 39791
        val server = listen("127.0.0.1", port)
        var serverConn: IoStream? = null
        val accepted = launch { serverConn = server.accept() }
        val client = connect("127.0.0.1", port)
        accepted.join()
        val worker = Worker.start()
        val outcome = worker.execute(TransferMode.SAFE, { client }) { c ->
            runBlocking { try { c.read(Buffer(16)); "read" } catch (t: IllegalStateException) { "rejected" } }
        }.result
        worker.requestTermination().result
        assertEquals("rejected", outcome)
        client.close(); serverConn!!.close(); server.close()
    }
}
