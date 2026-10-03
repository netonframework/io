package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** SPEC §23.3: vectored writes and half-close. */
class WritevTest {

    private suspend fun readExactly(s: IoStream, n: Int): ByteArray {
        val buf = Buffer()
        while (buf.readableBytes < n) { if (s.read(buf) < 0) break }
        return buf.readAll()
    }

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

    @Test
    fun manySmallAndEmptyBuffersArriveInOrder() = runReactor {
        pair(21890) { c, s ->
            val parts = Array(200) { i -> Buffer().also { b -> repeat(if (i % 7 == 0) 0 else i % 13 + 1) { b.writeByte((i + it).toByte()) } } }
            val expected = parts.flatMap { it.peekAll().toList() }.toByteArray()
            val reader = async { readExactly(s, expected.size) }
            val n = c.writev(parts)
            assertEquals(expected.size.toLong(), n)
            assertTrue(parts.all { it.readableBytes == 0 }, "every buffer must be consumed")
            assertContentEquals(expected, reader.await())
        }
    }

    /** 12 MB in three buffers exceeds the socket buffers: the write must park on would-block and resume. */
    @Test
    fun largeVectoredWriteParksAndCompletes() = runReactor {
        pair(21891) { c, s ->
            val parts = Array(3) { k -> Buffer(4 shl 20).also { b -> val a = ByteArray(4 shl 20) { i -> ((i * 31 + k) and 0xFF).toByte() }; b.writeBytes(a) } }
            val expected = parts.flatMap { it.peekAll().toList() }.toByteArray()
            val reader = async { delay(50); readExactly(s, expected.size) }   // start late so the writer fills the pipe
            assertEquals(expected.size.toLong(), c.writev(parts))
            assertContentEquals(expected, reader.await())
        }
    }

    @Test
    fun halfCloseLetsThePeerSeeEofAndStillAnswer() = runReactor {
        pair(21892) { c, s ->
            val out = Buffer(); out.writeBytes("hello".encodeToByteArray())
            c.write(out)
            c.shutdownOutput()
            // Server: everything up to EOF, then the peer can still be written to.
            val got = Buffer()
            while (s.read(got) >= 0) { /* until EOF */ }
            assertEquals("hello", got.readAll().decodeToString())
            val reply = Buffer(); reply.writeBytes("bye".encodeToByteArray())
            s.write(reply)
            s.shutdownOutput()
            // Client: half-closed for writing, still reads the reply and then EOF.
            val back = Buffer()
            while (c.read(back) >= 0) { /* until EOF */ }
            assertEquals("bye", back.readAll().decodeToString())
        }
    }

    @Test
    fun writevAfterCloseThrowsClosedException() = runReactor {
        pair(21893) { c, _ ->
            c.close()
            val b = Buffer(); b.writeBytes(byteArrayOf(1))
            assertFailsWith<ClosedException> { c.writev(arrayOf(b)) }
        }
    }
}
