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
