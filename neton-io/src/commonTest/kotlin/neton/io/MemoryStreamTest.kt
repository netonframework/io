package neton.io

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.ClosedException
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.core.memoryStreamPairWithHook
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §27.4: in-memory stream pair semantics. */
class MemoryStreamTest {
    private fun buf(s: String) = Buffer().also { it.writeBytes(s.encodeToByteArray()) }

    @Test
    fun framedEchoOverMemory() = runBlocking {
        val (client, server) = memoryStreamPair()
        val srv = launch {
            val f = Framed(Io(server), LineCodec, LineCodec)
            f.incoming().collect { f.send("echo:$it") }
        }
        val c = Framed(Io(client), LineCodec, LineCodec)
        c.send("hi")
        assertEquals("echo:hi", c.incoming().first())
        client.close()
        withTimeout(2_000) { srv.join() }                     // server sees EOF and ends
    }

    @Test
    fun writerSuspendsAtCapacityUntilTheReaderReads() = runBlocking {
        val (a, b) = memoryStreamPair(capacity = 8)
        val w = async { a.write(buf("0123456789ABCDEF")) }   // 16 bytes into an 8-byte pipe
        delay(50)
        assertTrue(w.isActive, "the write must wait for room")
        val got = StringBuilder()
        val r = Buffer()
        while (got.length < 16) { r.clear(); b.read(r); got.append(r.readAll().decodeToString()) }
        assertEquals(16, withTimeout(2_000) { w.await() })
        assertEquals("0123456789ABCDEF", got.toString())
    }

    @Test
    fun shutdownOutputGivesThePeerEofAfterTheData() = runBlocking {
        val (a, b) = memoryStreamPair()
        a.write(buf("last"))
        a.shutdownOutput()
        val r = Buffer()
        assertEquals(4, b.read(r)); assertEquals("last", r.readAll().decodeToString())
        assertEquals(-1, b.read(r))
        b.write(buf("still"))                                   // the other direction stays open
        assertEquals(5, a.read(r))
    }

    @Test
    fun closeEndsBothDirections() = runBlocking {
        val (a, b) = memoryStreamPair()
        val parked = async { runCatching { a.read(Buffer()) } }
        delay(20)
        a.close()
        assertTrue(withTimeout(2_000) { parked.await() }.exceptionOrNull() is ClosedException)
        assertEquals(-1, b.read(Buffer()))                      // peer: EOF
        val e = runCatching { b.write(buf("x")) }.exceptionOrNull()
        assertTrue(e is IoException, "writing to a closed peer must fail, got $e")
    }

    @Test
    fun usableAcrossThreads() = runBlocking {
        val (a, b) = memoryStreamPair(capacity = 1024)
        val ctx = newSingleThreadContext("memory-peer")
        val n = 200_000
        val reader = async(ctx) {
            var total = 0; val r = Buffer()
            while (total < n) { r.clear(); val k = b.read(r); if (k < 0) break; total += k }
            total
        }
        val chunk = ByteArray(1000) { 'x'.code.toByte() }
        repeat(n / 1000) { a.write(Buffer().also { it.writeBytes(chunk) }) }
        assertEquals(n, withTimeout(10_000) { reader.await() })
        ctx.close()
    }

    @Test
    fun writeAfterShutdownOutputFails() = runBlocking {
        val (a, b) = memoryStreamPair()
        a.write(buf("done"))
        a.shutdownOutput()
        val e = runCatching { a.write(buf("late")) }.exceptionOrNull()
        assertTrue(e is IoException && e !is ClosedException, "a write after shutdownOutput must fail, got $e")
        val r = Buffer()
        assertEquals(4, b.read(r)); assertEquals("done", r.readAll().decodeToString())
        assertEquals(-1, b.read(r))                               // nothing after EOF
    }

    @Test
    fun shutdownOutputFailsAParkedWrite() = runBlocking {
        val (a, b) = memoryStreamPair(capacity = 4)
        val w = async { runCatching { a.write(buf("12345678")) } }  // 4 bytes go in, then it parks
        delay(30)
        a.shutdownOutput()
        assertTrue(withTimeout(2_000) { w.await() }.exceptionOrNull() is IoException)
        val r = Buffer()
        assertEquals(4, b.read(r)); assertEquals("1234", r.readAll().decodeToString())
        assertEquals(-1, b.read(r))
    }

    /** A close landing between the open check and the waiter registration (via the test hook). */
    private fun closingPair(closeWhich: (Pair<IoStream, IoStream>) -> IoStream): Pair<IoStream, IoStream> {
        var armed = true
        var pair: Pair<IoStream, IoStream>? = null
        pair = memoryStreamPairWithHook(capacity = 4) { if (armed) { armed = false; closeWhich(pair!!).close() } }
        return pair
    }

    @Test
    fun readClosedInTheCheckWindowFailsInsteadOfHanging() = runBlocking {
        val (a, _) = closingPair { it.first }
        val e = withTimeout(2_000) { runCatching { a.read(Buffer()) }.exceptionOrNull() }
        assertTrue(e is ClosedException, "got $e")
    }

    @Test
    fun writeClosedInTheCheckWindowFailsInsteadOfHanging() = runBlocking {
        val (a, _) = closingPair { it.first }
        // Fits the capacity: one pass, so only the check under the lock can catch the close.
        val e = withTimeout(2_000) { runCatching { a.write(buf("12")) }.exceptionOrNull() }
        assertTrue(e is ClosedException, "got $e")
    }

    @Test
    fun peerClosedInTheCheckWindowGivesEof() = runBlocking {
        val (a, _) = closingPair { it.second }
        assertEquals(-1, withTimeout(2_000) { a.read(Buffer()) })
    }
}
