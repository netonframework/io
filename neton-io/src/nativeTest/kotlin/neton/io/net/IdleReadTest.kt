package neton.io.net

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A read parked across the idle sweep (50 ms; readiness drivers give the pooled array back then)
 * must still get the data, EOF, its cancellation, or the close — and more than once (SPEC §24, §26.5).
 */
class IdleReadTest {
    /** Parked across several idle sweeps. */
    private val IDLE = 300L

    private suspend fun pair(port: Int): Pair<IoStream, IoStream> {
        val l = listen("127.0.0.1", port)
        val client = kotlinx.coroutines.coroutineScope { val c = async { connect("127.0.0.1", port) }; val s = l.accept(); l.close(); s to c.await() }
        return client
    }

    private suspend fun send(s: IoStream, text: String) { val b = Buffer(); b.writeBytes(text.encodeToByteArray()); s.write(b) }

    @Test
    fun idleReadStillReceivesRepeatedly() = runReactor {
        val (server, client) = pair(21930)
        val buf = Buffer(pooled = true)
        for (round in 1..3) {
            val reader = async { buf.clear(); val n = server.read(buf); n to buf.readBytes(buf.readableBytes).decodeToString() }
            delay(IDLE)                                          // parked across several sweeps
            send(client, "r$round")
            val (n, text) = withTimeout(2_000) { reader.await() }
            assertEquals(2, n); assertEquals("r$round", text)
        }
        client.close(); server.close()
    }

    @Test
    fun idleReadSeesEof() = runReactor {
        val (server, client) = pair(21931)
        val reader = async { server.read(Buffer(pooled = true)) }
        delay(IDLE)
        client.close()
        assertEquals(-1, withTimeout(2_000) { reader.await() })
        server.close()
    }

    @Test
    fun idleReadCanBeCancelled() = runReactor {
        val (server, client) = pair(21932)
        var cancelled = false
        val job = launch {
            try { server.read(Buffer(pooled = true)); fail("read returned") } catch (e: CancellationException) { cancelled = true; throw e }
        }
        delay(IDLE)
        job.cancel()
        withTimeout(2_000) { job.join() }
        assertTrue(cancelled)
        // The stream still works after a cancelled idle read.
        val reader = async { val b = Buffer(); server.read(b); b.readBytes(b.readableBytes).decodeToString() }
        send(client, "ok")
        assertEquals("ok", withTimeout(2_000) { reader.await() })
        client.close(); server.close()
    }

    @Test
    fun idleReadEndsWhenItsStreamCloses() = runReactor {
        val (server, client) = pair(21933)
        val reader = async { runCatching { server.read(Buffer(pooled = true)) } }
        delay(IDLE)
        server.close()
        val r = withTimeout(2_000) { reader.await() }
        assertTrue(r.isFailure || r.getOrNull() == -1, "a closed stream's parked read must end, got $r")
        client.close()
    }
}
