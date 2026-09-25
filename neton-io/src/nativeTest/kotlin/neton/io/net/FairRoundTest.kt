package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * SPEC §19.5: a connection gets at most one successful recv per poll round; a second read in the
 * same round waits for the next round. (On the poll(2) and kqueue/epoll readiness drivers; the
 * io_uring driver has its own path and these hold there trivially.)
 */
class FairRoundTest {

    private suspend fun send(s: IoStream, text: String) { val b = Buffer(); b.writeBytes(text.encodeToByteArray()); s.write(b) }

    /** A reader deferred to the next round still gets the data that arrives. */
    @Test
    fun secondReadInTheSameRoundIsServedNextRound() = runReactor {
        val server = listen("127.0.0.1", 21870)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(server.accept()) }
        val client = connect("127.0.0.1", 21870)
        val conn = accepted.await()
        send(client, "a")
        val first = Buffer(); conn.read(first)                    // served this round
        val second = async { val b = Buffer(); conn.read(b); b.readBytes(b.readableBytes).decodeToString() }
        yield()                                                    // `second` runs now and defers
        send(client, "b")
        assertEquals("a", first.readBytes(first.readableBytes).decodeToString())
        assertEquals("b", withTimeout(2_000) { second.await() })
        conn.close(); client.close(); server.close()
    }

    /**
     * Closing a stream whose reader is deferred must fail that reader with ClosedException: it is
     * not in a waiter slot, and letting it recv later could hit a new connection on the same fd.
     */
    @Test
    fun closeFailsADeferredReader() = runReactor {
        val server = listen("127.0.0.1", 21871)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(server.accept()) }
        val client = connect("127.0.0.1", 21871)
        val conn = accepted.await()
        send(client, "a")
        conn.read(Buffer())                                        // served this round
        // The outcome is captured inside the coroutine: a failing child would otherwise cancel
        // the test's scope before await() could observe it.
        val second = async { runCatching { conn.read(Buffer()) }.exceptionOrNull() }
        yield()                                                    // `second` is deferred now
        conn.close()
        val failure = withTimeout(2_000) { second.await() }
        assertTrue(failure is ClosedException, "expected ClosedException, got $failure")
        client.close(); server.close()
    }
}
