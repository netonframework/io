package neton.io.net

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import kotlin.test.Test
import kotlin.test.assertEquals

/** One connection's failure (a throwing handler, a peer reset) ends that connection only. */
class ConnectionFaultTest {

    @Test
    fun throwingHandlerAndPeerResetDoNotStopTheServer() = runReactor {
        val g = listenGroup("127.0.0.1", 21950, reactors = 1)
        val serveJob = launch {
            g.serve { conn ->
                val f = Framed(Io(conn), LineCodec, LineCodec)
                f.receiveEach { line ->
                    if (line == "boom") throw IllegalStateException("handler failed on purpose")
                    f.send(line)
                }
                conn.close()
            }
        }
        val good = connect("127.0.0.1", 21950)
        val goodF = Framed(Io(good), LineCodec, LineCodec)
        goodF.send("one")
        assertEquals("one", goodF.incoming().first())

        // A handler that throws.
        val bad = connect("127.0.0.1", 21950)
        Framed(Io(bad), LineCodec, LineCodec).send("boom")
        // A peer that resets (SO_LINGER 0 then close sends RST, as macOS `nc -z` does).
        val rst = connect("127.0.0.1", 21950, SocketOptions(lingerSeconds = 0))
        Framed(Io(rst), LineCodec, LineCodec).send("x")
        rst.close()
        delay(200)

        // The first connection, on the same reactor, still works.
        goodF.send("two")
        assertEquals("two", Framed(Io(good), LineCodec, LineCodec).incoming().first())
        bad.close(); good.close()
        g.shutdown(1_000); serveJob.join(); g.awaitWorkers()
    }
}
