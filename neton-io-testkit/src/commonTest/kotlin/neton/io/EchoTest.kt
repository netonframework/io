package neton.io

import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.BaseFilter
import neton.io.core.Filter
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import neton.io.core.serve
import neton.io.testkit.memoryPair
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * P0 acceptance: the full bytes + codec + Io + Filter + Framed + Service + dispatcher path
 * exercised over the in-memory driver, in pure Kotlin/Native with no cinterop.
 */
class EchoTest {

    /** Demo filter: uppercases lowercase bytes on read, proving the filter stack works end to end (TLS goes here later). */
    private class UppercaseFilter(private val inner: IoStream) : Filter {
        override suspend fun read(dst: Buffer): Int {
            val tmp = Buffer()
            val n = inner.read(tmp)
            if (n <= 0) return n
            val src = tmp.readAll()
            for (i in src.indices) {
                val c = src[i]
                if (c in 'a'.code.toByte()..'z'.code.toByte()) src[i] = (c - 32).toByte()
            }
            dst.writeBytes(src)
            return src.size
        }
        override suspend fun write(src: Buffer): Int = inner.write(src)
        override suspend fun flush() = inner.flush()
        override fun close() = inner.close()
    }

    @Test
    fun echoLine() = runBlocking {
        val (client, server) = memoryPair()

        val serverFramed = Framed(Io.of(server, ::BaseFilter), LineCodec, LineCodec)
        val serverJob = launch { serve(serverFramed) { req -> "echo:$req" } }

        val clientFramed = Framed(Io(client), LineCodec, LineCodec)
        clientFramed.send("hello")
        val reply = clientFramed.incoming().first()
        assertEquals("echo:hello", reply)

        client.close()
        serverJob.cancelAndJoin()
    }

    @Test
    fun multipleFramesAndEof() = runBlocking {
        val (client, server) = memoryPair()

        val serverFramed = Framed(Io(server), LineCodec, LineCodec)
        val serverJob = launch { serve(serverFramed) { req -> req.uppercase() } }

        val clientFramed = Framed(Io(client), LineCodec, LineCodec)
        clientFramed.send("a")
        clientFramed.send("bb")
        clientFramed.send("ccc")

        // take(3) completes the flow after three frames; no dependency on the peer closing
        val replies = clientFramed.incoming().take(3).toList()
        assertEquals(listOf("A", "BB", "CCC"), replies)

        client.close()
        serverJob.cancelAndJoin()
    }

    @Test
    fun filterStackTransformsInbound() = runBlocking {
        val (client, server) = memoryPair()

        // the server stacks an UppercaseFilter on its inbound stream before the codec sees the bytes
        val serverFramed = Framed(Io.of(server, ::UppercaseFilter), LineCodec, LineCodec)
        val serverJob = launch { serve(serverFramed) { req -> "got:$req" } }

        val clientFramed = Framed(Io(client), LineCodec, LineCodec)
        clientFramed.send("hello")
        val reply = clientFramed.incoming().first()
        // the client sends "hello"; the server filter turns it into "HELLO"
        assertEquals("got:HELLO", reply)

        client.close()
        serverJob.cancelAndJoin()
    }
}
