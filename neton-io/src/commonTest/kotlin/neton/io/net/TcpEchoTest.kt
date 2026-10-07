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

// Test ports are deliberately below 32768, outside the kernel's ephemeral range
// (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999). A fixed listen port
// inside that range intermittently loses the bind to some other process's outbound
// connection, which SO_REUSEADDR does not help with — it surfaces as a flaky EADDRINUSE.
/**
 * P1 acceptance: the P0 model (Framed/Service/dispatcher) runs unchanged over a real
 * non-blocking TCP connection driven by the kqueue/epoll reactor.
 */
class TcpEchoTest {

    @Test
    fun echoOverTcp() = runReactor {
        val port = 19217
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
    fun manyConcurrentConnections() = runReactor {
        // Regression for an io_uring SQ-ring overflow: with more concurrent in-flight ops than the
        // ring held, submissions were overwritten and the reactor stalled (~200 connections).
        val port = 19219
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            while (true) {
                val conn = server.accept()
                launch { serve(Framed(Io(conn), LineCodec, LineCodec)) { it } }
            }
        }

        val n = 400
        // Connected one after another, then all in flight at once: 400 parked reads is what overflowed the ring. 400
        // simultaneous handshakes would instead test the platform's listen queue (macOS caps it at 128 and resets the rest).
        val connections = (1..n).map { connect("127.0.0.1", port) }
        val clients = (1..n).map { i ->
            launch {
                val client = connections[i - 1]
                val framed = Framed(Io(client), LineCodec, LineCodec)
                framed.send("m$i")
                assertEquals("m$i", framed.incoming().first())
                client.close()
            }
        }
        clients.forEach { it.join() }

        serverJob.cancelAndJoin()
        server.close()
    }

    @Test
    fun multipleFramesOverTcp() = runReactor {
        val port = 19218
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
