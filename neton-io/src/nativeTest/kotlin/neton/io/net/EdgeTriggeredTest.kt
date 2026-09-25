package neton.io.net

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * SPEC §17 (persistent edge-triggered read interest): the reactor must drain to EAGAIN before
 * parking, must not lose an edge that arrives while nobody is parked, and must wake a parked
 * reader on peer close.
 */
class EdgeTriggeredTest {

    // A connection from the previous test binary (another driver's run, seconds earlier) can still
    // hold the port; SO_REUSEADDR does not cover that case and it surfaced as a flaky EADDRINUSE
    // on 153 when the three driver suites run back to back. Retry the bind for a short while.
    private suspend fun listenRetrying(port: Int): TcpListener {
        var last: Throwable? = null
        repeat(50) {
            try { return listen("127.0.0.1", port) } catch (e: IllegalStateException) { last = e; delay(100) }
        }
        throw last!!
    }

    @Test
    fun coalescedFramesAreDrainedAcrossBursts() = runReactor {
        // Several frames written in one burst, then a pause, then more: with an edge per burst
        // the reader must drain the first burst fully (not park after the first frame) and pick
        // up the second burst from a fresh edge.
        val port = 21840
        val server = listenRetrying(port)
        var got: List<String> = emptyList()
        val srv = launch {
            val c = server.accept()
            got = Framed(Io(c), LineCodec, LineCodec).incoming().take(6).toList()
            c.close()
        }
        val cli = connect("127.0.0.1", port)
        val out = Buffer()
        out.writeBytes("a\nb\nc\n".encodeToByteArray()); cli.write(out)
        delay(50)
        out.clear(); out.writeBytes("d\ne\nf\n".encodeToByteArray()); cli.write(out)
        srv.join()
        assertEquals(listOf("a", "b", "c", "d", "e", "f"), got)
        cli.close(); server.close()
    }

    @Test
    fun edgeThatArrivesWhileNobodyIsParkedIsNotLost() = runReactor {
        // The server is busy (delaying) when data arrives; the edge must be remembered so the
        // later read returns immediately instead of parking forever.
        val port = 21841
        val server = listenRetrying(port)
        var line = ""
        val srv = launch {
            val c = server.accept()
            delay(100) // data lands during this; no reader parked
            line = Framed(Io(c), LineCodec, LineCodec).incoming().take(1).toList()[0]
            c.close()
        }
        val cli = connect("127.0.0.1", port)
        val out = Buffer(); out.writeBytes("late\n".encodeToByteArray()); cli.write(out)
        srv.join()
        assertEquals("late", line)
        cli.close(); server.close()
    }

    @Test
    fun parkedReaderWakesOnPeerClose() = runReactor {
        val port = 21842
        val server = listenRetrying(port)
        var eof = 0
        val srv = launch {
            val c: IoStream = server.accept()
            eof = c.read(Buffer(16)) // parks; peer closes -> -1
            c.close()
        }
        val cli = connect("127.0.0.1", port)
        delay(30)
        cli.close()
        srv.join()
        assertEquals(-1, eof)
        server.close()
    }
}
