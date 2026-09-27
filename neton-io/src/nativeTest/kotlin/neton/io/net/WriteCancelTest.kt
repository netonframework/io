package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A write cancelled mid-way leaves its buffer advanced by exactly what reached the socket (SPEC §24):
 * a successor writer (msgtrans' INLINE mode) continues from there, so the peer must see neither a gap
 * nor duplicated bytes. On io_uring the in-flight SEND is cancelled in the kernel and accounted
 * before the writer is resumed.
 */
class WriteCancelTest {

    @Test
    fun cancelledWriteAccountsExactlyWhatWasSent() = runReactor {
        val listener = listen("127.0.0.1", 21940)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(listener.accept()) }
        val c = connect("127.0.0.1", 21940)
        val s = accepted.await()
        val total = 32 shl 20
        val buf = Buffer(total)
        buf.writeBytes(ByteArray(total) { (it % 251).toByte() })
        val writer = launch { c.write(buf) }          // the peer does not read yet: this parks
        delay(100)
        writer.cancelAndJoin()
        val sent = total - buf.readableBytes
        assertTrue(sent in 1 until total, "expected a partial write, sent=$sent")
        // A successor finishes the job from where the buffer says the stream is.
        val finisher = launch { c.write(buf); c.shutdownOutput() }
        val got = Buffer(1 shl 20)
        var received = 0L
        var mismatch = -1L
        while (true) {
            got.clear()
            val n = s.read(got)
            if (n < 0) break
            val bytes = got.readAll()
            for (i in bytes.indices) if (mismatch < 0 && bytes[i] != ((received + i) % 251).toByte()) mismatch = received + i
            received += n
        }
        finisher.join()
        assertEquals(-1L, mismatch, "first byte that differs from the source")
        assertEquals(total.toLong(), received, "the peer must receive every byte exactly once")
        c.close(); s.close(); listener.close()
    }
}
