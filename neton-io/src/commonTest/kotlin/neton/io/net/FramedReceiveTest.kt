package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals

/** SPEC §24: Framed.receiveEach (pull loop) and inline send. */
class FramedReceiveTest {

    @Test
    fun receiveEachDeliversEveryFrameInOrderUntilEof() = runReactor {
        val listener = listen("127.0.0.1", 21930)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(listener.accept()) }
        val c = connect("127.0.0.1", 21930)
        val s = accepted.await()
        val got = async {
            val lines = ArrayList<String>()
            Framed(Io(s), LineCodec, LineCodec).receiveEach { lines.add(it) }
            lines
        }
        val out = Framed(Io(c), LineCodec, LineCodec)
        for (i in 1..50) out.send("m$i")                        // inline send in a loop
        val tail = Buffer(); tail.writeBytes("p1\np2\npar".encodeToByteArray())
        c.write(tail)                                            // several frames in one write, one partial
        val rest = Buffer(); rest.writeBytes("tial\n".encodeToByteArray())
        c.write(rest)
        c.shutdownOutput()
        assertEquals((1..50).map { "m$it" } + listOf("p1", "p2", "partial"), got.await())
        c.close(); s.close(); listener.close()
    }
}
