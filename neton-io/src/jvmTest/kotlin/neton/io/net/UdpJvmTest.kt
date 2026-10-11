package neton.io.net

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import neton.io.core.ClosedException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * UDP on the JVM (SPEC §37): the same API as native over a DatagramChannel, with what NIO cannot do declared — one
 * datagram per receive, no GSO / GRO, no ECN, no destination address.
 */
class UdpJvmTest {
    private suspend fun send(from: UdpSocket, to: SocketAddress, bytes: ByteArray) {
        Transmit().use { t ->
            bytes.copyInto(t.buffer); t.length = bytes.size; t.setDestination(to)
            from.send(t)
        }
    }

    private suspend fun receive(on: UdpSocket, b: RecvBatch): ByteArray {
        assertEquals(1, on.recv(b))
        return b.buffer.copyOfRange(b.offset(0), b.offset(0) + b.length(0))
    }

    @Test
    fun capabilitiesAreDeclared() = runReactor {
        val s = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        assertEquals(1, BATCH_SIZE)
        assertEquals(1, s.maxGsoSegments)
        assertEquals(1, s.groSegments)
        assertTrue(s.mayFragment)
        s.close()
    }

    @Test
    fun ipv4RoundTripWithAddressesAndPorts() = runReactor {
        val a = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val b = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        assertTrue(a.localAddress.port > 0)
        assertEquals(SocketAddress.ipv4(127, 0, 0, 1, a.localAddress.port), a.localAddress)
        RecvBatch(1).use { batch ->
            send(a, b.localAddress, "hello".encodeToByteArray())
            assertEquals("hello", receive(b, batch).decodeToString())
            assertEquals(a.localAddress, batch.source(0))
            assertTrue(batch.sourceEquals(0, a.localAddress))
            assertEquals(5, batch.stride(0))
            assertNull(batch.ecn(0))
            assertNull(batch.destination(0))
        }
        a.close(); b.close()
    }

    @Test
    fun dualStackSeesIpv4PeersAsMapped() = runReactor {
        val v6 = bindUdp(SocketAddress.IPV6_UNSPECIFIED_ANY_PORT, UdpOptions(ipv6Only = false))
        val v4 = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        RecvBatch(1).use { batch ->
            send(v4, SocketAddress.ipv4(127, 0, 0, 1, v6.localAddress.port), byteArrayOf(1, 2, 3))
            assertTrue(receive(v6, batch).contentEquals(byteArrayOf(1, 2, 3)))
            assertEquals(SocketAddress.ipv4(127, 0, 0, 1, v4.localAddress.port).toIpv4Mapped(), batch.source(0))
            // ...and answers it at that address.
            send(v6, batch.source(0), byteArrayOf(4))
            assertTrue(receive(v4, batch).contentEquals(byteArrayOf(4)))
        }
        v6.close(); v4.close()
    }

    @Test
    fun manyDatagramsOneByOne() = runReactor {
        val a = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val b = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        val n = 200
        RecvBatch(1).use { batch ->
            val seen = BooleanArray(n)
            repeat(n) { i -> send(a, b.localAddress, byteArrayOf(i.toByte(), (i shr 8).toByte())) }
            repeat(n) {
                val got = receive(b, batch)
                seen[(got[0].toInt() and 0xff) or ((got[1].toInt() and 0xff) shl 8)] = true
            }
            assertTrue(seen.all { it })
        }
        a.close(); b.close()
    }

    @Test
    fun buffersCanBeSet() = runReactor {
        val s = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT, UdpOptions(sendBufferSize = 123456, receiveBufferSize = 123456))
        assertTrue(s.sendBufferSize() >= 123456, "send buffer ${s.sendBufferSize()}")
        assertTrue(s.receiveBufferSize() >= 123456, "receive buffer ${s.receiveBufferSize()}")
        s.close()
    }

    @Test
    fun closeWakesAParkedReceive() = runReactor {
        val s = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        val batch = RecvBatch(1)
        val parked = async { runCatching { s.recv(batch) }.exceptionOrNull() }
        delay(50)
        s.close()
        assertTrue(parked.await() is ClosedException, "got ${parked.await()}")
        batch.close()
        assertFailsWith<ClosedException> { s.recv(RecvBatch(1)) }
    }

    @Test
    fun anOversizedDatagramCountsAsSent() = runReactor {
        val a = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val b = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        Transmit(65535).use { t ->
            t.length = 65535; t.setDestination(b.localAddress)                   // > 65507, the IPv4 UDP maximum
            a.send(t)
        }
        a.close(); b.close()
    }
}
