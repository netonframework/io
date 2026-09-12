package neton.io.net

import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.serve
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * P1 acceptance: the P0 model (Framed/Service/dispatcher) runs unchanged over a real
 * non-blocking TCP connection driven by the kqueue/epoll reactor.
 */
class TcpEchoTest {

    @Test
    fun echoOverTcp() = runReactor {
        val port = 39217
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            val conn = server.accept()
            serve(Framed(Io(conn), LineCodec, LineCodec)) { req -> "echo:$req" }
        }

        val client = connect("127.0.0.1", port)
        val framed = Framed(Io(client), LineCodec, LineCodec)
        framed.send("hello")
        assertEquals("echo:hello", framed.incoming().first())

        client.close()
        serverJob.cancelAndJoin()
        server.close()
    }

    @Test
    fun multipleFramesOverTcp() = runReactor {
        val port = 39218
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            val conn = server.accept()
            serve(Framed(Io(conn), LineCodec, LineCodec)) { req -> req.uppercase() }
        }

        val client = connect("127.0.0.1", port)
        val framed = Framed(Io(client), LineCodec, LineCodec)
        framed.send("a")
        framed.send("bb")
        framed.send("ccc")
        assertEquals(listOf("A", "BB", "CCC"), framed.incoming().take(3).toList())

        client.close()
        serverJob.cancelAndJoin()
        server.close()
    }
}
