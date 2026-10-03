package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** `IoStream.peerAddress` / `localAddress`: each end's peer is the other end's local address. */
class PeerAddressTest {

    private suspend fun pairOn(host: String, port: Int, body: suspend (client: IoStream, accepted: IoStream) -> Unit) {
        val server = listen(host, port)
        val accepted = CompletableDeferred<IoStream>()
        kotlinx.coroutines.coroutineScope {
            launch { accepted.complete(server.accept()) }
            val c = connect(host, port)
            val s = accepted.await()
            try { body(c, s) } finally { c.close(); s.close(); server.close() }
        }
    }

    @Test
    fun ipv4EndsSeeEachOther() = runReactor {
        pairOn("127.0.0.1", 21872) { c, s ->
            val clientLocal = assertNotNull(c.localAddress)
            val clientPeer = assertNotNull(c.peerAddress)
            assertEquals(SocketAddress.ipv4(127, 0, 0, 1, 21872), clientPeer)
            assertEquals(clientLocal, s.peerAddress)
            assertEquals(clientPeer, s.localAddress)
            assertTrue(clientLocal.isIpv4 && clientLocal.port != 0 && clientLocal.port != 21872, "$clientLocal")
        }
    }

    @Test
    fun ipv6EndsSeeEachOther() = runReactor {
        pairOn("::1", 21873) { c, s ->
            val peer = assertNotNull(s.peerAddress)
            assertTrue(peer.isIpv6, "$peer")
            assertEquals(c.localAddress, peer)
            assertEquals("[0:0:0:0:0:0:0:1]:21873", c.peerAddress.toString())
        }
    }

    /** A dual-stack listener reports an IPv4 client as v4-mapped; `toCanonical` gives the IPv4 address. */
    @Test
    fun dualStackPeerIsMapped() = runReactor {
        val server = listen("::", 21874)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(server.accept()) }
        val c = connect("127.0.0.1", 21874)
        val s = accepted.await()
        val peer = assertNotNull(s.peerAddress)
        assertTrue(peer.isIpv4Mapped, "$peer")
        assertEquals(c.localAddress, peer.toCanonical())
        c.close(); s.close(); server.close()
    }

    /** The cases of Rust std's `Ipv6Addr` display tests (RFC 5952). */
    @Test
    fun ipStringMatchesRustDisplay() {
        fun v6(vararg g: Int, scope: Int = 0) =
            SocketAddress.of(ByteArray(16) { i -> (g[i / 2] shr (if (i % 2 == 0) 8 else 0)).toByte() }, 0, scope).ipString()
        assertEquals("127.0.0.1", SocketAddress.ipv4(127, 0, 0, 1, 80).ipString())
        assertEquals("::", v6(0, 0, 0, 0, 0, 0, 0, 0))
        assertEquals("::1", v6(0, 0, 0, 0, 0, 0, 0, 1))
        assertEquals("1::", v6(1, 0, 0, 0, 0, 0, 0, 0))
        assertEquals("1:0:0:4::8", v6(1, 0, 0, 4, 0, 0, 0, 8))          // the longer run
        assertEquals("1::4:5:0:0:8", v6(1, 0, 0, 4, 5, 0, 0, 8))        // a tie: the first
        assertEquals("1:0:2:3:4:5:6:7", v6(1, 0, 2, 3, 4, 5, 6, 7))     // one zero group stays
        assertEquals("2001:db8::ff00:42:8329", v6(0x2001, 0xdb8, 0, 0, 0, 0xff00, 0x42, 0x8329))
        assertEquals("::ffff:192.0.2.128", v6(0, 0, 0, 0, 0, 0xffff, 0xc000, 0x280))
        assertEquals("fe80::1%3", v6(0xfe80, 0, 0, 0, 0, 0, 0, 1, scope = 3))
    }

    @Test
    fun nonSocketStreamsHaveNoAddress() {
        val (a, b) = memoryStreamPair()
        assertNull(a.peerAddress)
        assertNull(b.localAddress)
    }

    @Test
    fun closedStreamHasNoAddress() = runReactor {
        pairOn("127.0.0.1", 21875) { c, _ ->
            c.close()
            assertNull(c.peerAddress)
            assertNull(c.localAddress)
        }
    }
}
