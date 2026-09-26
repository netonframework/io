package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.ClosedException
import neton.io.core.FrameReadRate
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import neton.io.core.Service
import neton.io.core.TimeoutException
import neton.io.core.closeGracefully
import neton.io.core.serve
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/** SPEC §23.2: timeouts, frame read rate, graceful close, batched serve. */
class TimeoutTest {

    private suspend fun pair(port: Int, body: suspend (client: IoStream, server: IoStream) -> Unit) {
        val listener = listen("127.0.0.1", port)
        val accepted = CompletableDeferred<IoStream>()
        kotlinx.coroutines.coroutineScope {
            launch { accepted.complete(listener.accept()) }
            val c = connect("127.0.0.1", port)
            val s = accepted.await()
            try { body(c, s) } finally { c.close(); s.close(); listener.close() }
        }
    }

    private fun text(s: String) = Buffer().also { it.writeBytes(s.encodeToByteArray()) }

    @Test
    fun readTimeoutFiresAndTheStreamStaysUsable() = runReactor {
        pair(21900) { c, s ->
            s.setTimeouts(readTimeoutMillis = 100)
            val start = TimeSource.Monotonic.markNow()
            val first = runCatching { s.read(Buffer()) }.exceptionOrNull()
            val ms = start.elapsedNow().inWholeMilliseconds
            assertTrue(first is TimeoutException, "expected TimeoutException, got $first")
            assertTrue(ms in 80..1500, "timed out after $ms ms for a 100 ms timeout")
            c.write(text("after"))
            val b = Buffer(); s.read(b)
            assertEquals("after", b.readAll().decodeToString())
        }
    }

    @Test
    fun writeTimeoutFiresWhenThePeerStopsReading() = runReactor {
        pair(21901) { c, _ ->
            c.setTimeouts(writeTimeoutMillis = 200)
            val big = Buffer(64 shl 20); big.writeBytes(ByteArray(64 shl 20))  // far beyond the socket buffers
            val r = runCatching { c.write(big) }.exceptionOrNull()
            assertTrue(r is TimeoutException, "expected TimeoutException, got $r")
        }
    }

    @Test
    fun idleTimeoutClosesAnIdleStream() = runReactor {
        pair(21902) { c, s ->
            s.setTimeouts(idleTimeoutMillis = 150)
            val parked = runCatching { s.read(Buffer()) }.exceptionOrNull()
            assertTrue(parked is TimeoutException, "parked read: expected TimeoutException, got $parked")
            assertTrue(runCatching { s.read(Buffer()) }.exceptionOrNull() is ClosedException, "stream must be closed")
            assertEquals(-1, withTimeout(2_000) { c.read(Buffer()) }, "peer sees EOF")
        }
    }

    @Test
    fun activityKeepsAnIdleTimeoutFromFiring() = runReactor {
        pair(21903) { c, s ->
            s.setTimeouts(idleTimeoutMillis = 200)
            val reader = async { val b = Buffer(); var got = 0; repeat(8) { got += s.read(b) }; got }
            repeat(8) { c.write(text("x")); delay(60) }                          // 480 ms total, never 200 ms quiet
            assertTrue(reader.await() >= 8)
        }
    }

    /** A line trickled one byte every 150 ms against a 100 ms / 400 ms-max rate of 4 bytes is cut off. */
    @Test
    fun frameReadRateCutsOffASlowSender() = runReactor {
        pair(21904) { c, s ->
            val framed = Framed(Io(s), LineCodec, LineCodec, readRate = FrameReadRate(100, 400, 4))
            val collected = async { runCatching { framed.incoming().toList() }.exceptionOrNull() }
            c.write(text("a"))
            repeat(6) { delay(150); runCatching { c.write(text("b")) } }
            assertTrue(collected.await() is TimeoutException, "slow frame must time out")
        }
    }

    @Test
    fun frameReadRateLetsANormalSenderThrough() = runReactor {
        pair(21905) { c, s ->
            val framed = Framed(Io(s), LineCodec, LineCodec, readRate = FrameReadRate(200, 1000, 4))
            val lines = async { framed.incoming().toList() }
            c.write(text("hel")); delay(50); c.write(text("lo\nworld\n")); c.shutdownOutput()
            assertEquals(listOf("hello", "world"), lines.await())
        }
    }

    @Test
    fun closeGracefullyLetsThePeerFinish() = runReactor {
        pair(21906) { c, s ->
            val peer = async { val b = Buffer(); while (c.read(b) >= 0) {}; c.write(text("late")); c.close(); b.readAll().decodeToString() }
            s.write(text("bye"))
            val start = TimeSource.Monotonic.markNow()
            s.closeGracefully(2_000)
            assertEquals("bye", peer.await())
            assertTrue(start.elapsedNow().inWholeMilliseconds < 1_500, "must return once the peer closed")
        }
    }

    /** 16 pipelined requests in one write are all answered, in order (SPEC §23.2 batched flush). */
    @Test
    fun pipelinedRequestsAreAnsweredInOrder() = runReactor {
        pair(21907) { c, s ->
            // serve returns at the client's EOF; half-closing then gives the client its EOF.
            launch { serve(Framed(Io(s), LineCodec, LineCodec), Service<String, String> { "echo:$it" }); s.shutdownOutput() }
            c.write(text((1..16).joinToString("") { "r$it\n" }))
            c.shutdownOutput()
            val got = Framed(Io(c), LineCodec, LineCodec).incoming().toList()
            assertEquals((1..16).map { "echo:r$it" }, got)
        }
    }
}
