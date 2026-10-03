package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.TimeSource

// Test ports are deliberately below 32768, outside the kernel's ephemeral range
// (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999). A fixed listen port
// inside that range intermittently loses the bind to some other process's outbound
// connection, which SO_REUSEADDR does not help with — it surfaces as a flaky EADDRINUSE.
/** Threading contract: cross-thread resume is supported and wakes the loop; I/O off-thread is rejected; timers stay on the reactor. */
class ThreadingTest {

    @Test
    fun resumeFromAnotherThreadWakesTheReactor() = runReactor {
        val owner = currentThreadId()
        val gate = CompletableDeferred<String>()
        val worker = startTestWorker()
        worker.submit {
            sleepMicros(50_000)
            gate.complete("from-worker") // resumes the awaiter: dispatch() from a foreign thread
        }
        val t0 = TimeSource.Monotonic.markNow()
        val v = gate.await() // the loop is parked in the poller; the self-pipe must wake it
        assertEquals("from-worker", v)
        assertEquals(owner, currentThreadId(), "continuation must run on the reactor thread")
        assertTrue(t0.elapsedNow().inWholeMilliseconds < 5_000, "reactor was not woken promptly")
        worker.stop()
    }

    @Test
    fun delayAndWithTimeoutRunOnTheReactorThread() = runReactor {
        val owner = currentThreadId()
        val t0 = TimeSource.Monotonic.markNow()
        delay(30)
        val e = t0.elapsedNow().inWholeMilliseconds
        assertTrue(e in 25..2_000, "delay(30) took ${e}ms")
        assertEquals(owner, currentThreadId())

        val port = 19790
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
        val port = 19791
        val server = listen("127.0.0.1", port)
        var serverConn: IoStream? = null
        val accepted = launch { serverConn = server.accept() }
        val client = connect("127.0.0.1", port)
        accepted.join()
        val worker = startTestWorker()
        val outcome = worker.submit {
            runBlocking { try { client.read(Buffer(16)); "read" } catch (t: IllegalStateException) { "rejected" } }
        }()
        worker.stop()
        assertEquals("rejected", outcome)
        client.close(); serverConn!!.close(); server.close()
    }
}
