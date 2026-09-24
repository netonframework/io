package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §16: connections are handed round-robin to reactors on their own threads and echo correctly. */
class MultiReactorTest {

    @Test
    fun connectionsAreServedAcrossReactorsAndEchoCorrectly() {
        val port = 39830
        val stop = CompletableDeferred<Unit>()
        val threadsSeen = kotlin.concurrent.AtomicReference<Set<ULong>>(emptySet())
        // Server on its own thread (serveTcp blocks): 2 reactors, line echo, records handler thread.
        val server = Worker.start(name = "srv")
        server.executeAfter(0L) {
            serveTcp("127.0.0.1", port, reactors = 2, until = stop) { conn: IoStream ->
                val t = currentThreadId()
                while (true) { val cur = threadsSeen.value; if (threadsSeen.compareAndSet(cur, cur + t)) break }
                try {
                    val framed = Framed(Io(conn), LineCodec, LineCodec)
                    framed.incoming().collect { framed.send(it) }
                } finally { conn.close() }
            }
        }
        // Clients from a separate reactor: 8 connections, each round-trips its own line.
        runReactor {
            for (_i in 1..100) { if (tryConnectProbe(port)) break; kotlinx.coroutines.delay(10) }
            val jobs = (1..8).map { i -> launch {
                val c = connect("127.0.0.1", port)
                val f = Framed(Io(c), LineCodec, LineCodec)
                f.send("hello-$i")
                assertEquals("hello-$i", f.incoming().first())
                c.close()
            } }
            jobs.forEach { it.join() }
        }
        stop.complete(Unit)
        server.requestTermination(processScheduledJobs = true).result
        assertTrue(threadsSeen.value.size >= 2, "expected connections on >=2 reactor threads, saw ${threadsSeen.value.size}")
    }

    private suspend fun tryConnectProbe(port: Int): Boolean =
        try { connect("127.0.0.1", port).also { it.close() }; true } catch (_: ConnectException) { false }
}
