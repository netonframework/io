@file:OptIn(kotlin.experimental.ExperimentalNativeApi::class)

package neton.io.net

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import neton.io.core.ClosedException
import kotlin.native.OsFamily
import kotlin.native.Platform
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §29.5: quinn-udp 0.11 `tests/tests.rs`, test for test, plus the extra checks of §29.5. */
class UdpTest {

    private suspend fun bindLocal(): UdpSocket =
        runCatching { bindUdp(SocketAddress.IPV6_LOCALHOST_ANY_PORT) }.getOrElse { bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT) }

    private class Tx(val destination: SocketAddress, val ecn: EcnCodepoint?, val contents: ByteArray, val segmentSize: Int = 0, val srcIp: SocketAddress? = null)

    /** quinn-udp `test_send_recv`. */
    private suspend fun testSendRecv(send: UdpSocket, recv: UdpSocket, tx: Tx) {
        // Windows allows 512 GSO segments: 128 x 512 bytes is one more than the default capacity.
        Transmit(maxOf(65535, tx.contents.size)).use { t ->
            tx.contents.copyInto(t.buffer); t.length = tx.contents.size
            t.setDestination(tx.destination); t.ecn = tx.ecn; t.segmentSize = tx.segmentSize; t.setSource(tx.srcIp)
            assertTrue(send.trySend(t))
        }
        val segmentSize = if (tx.segmentSize > 0) tx.segmentSize else tx.contents.size
        val expected = tx.contents.size / segmentSize
        var datagrams = 0
        RecvBatch(1).use { b ->
            while (datagrams < expected) {
                assertEquals(1, recv.recv(b))
                val segments = b.length(0) / b.stride(0)
                for (i in 0 until segments) {
                    val got = b.buffer.copyOfRange(b.offset(0) + i * b.stride(0), b.offset(0) + (i + 1) * b.stride(0))
                    val want = tx.contents.copyOfRange((datagrams + i) * segmentSize, (datagrams + i + 1) * segmentSize)
                    assertTrue(got.contentEquals(want), "segment ${datagrams + i} differs")
                }
                datagrams += segments
                assertEquals(send.localAddress.port, b.sourcePort(0))
                val sendV6 = send.localAddress.isIpv6; val recvV6 = recv.localAddress.isIpv6
                val addresses = listOfNotNull(b.source(0), b.destination(0))
                val v4Localhost = SocketAddress.ipv4(127, 0, 0, 1, 0)
                val v6Localhost = SocketAddress.IPV6_LOCALHOST_ANY_PORT
                for (a0 in addresses) {
                    val a = SocketAddress.of(a0.ipBytes(), 0, a0.scopeId)
                    when {
                        !recvV6 -> assertEquals(v4Localhost, a)
                        // Windows gives real IPv4 addresses, *nix v4-mapped: canonicalise to v4-mapped (reference).
                        !sendV6 -> assertEquals(v4Localhost.toIpv4Mapped(), a.toIpv4Mapped())
                        else -> assertTrue(a == v6Localhost || a == v4Localhost.toIpv4Mapped(), "unexpected address $a")
                    }
                }
                assertEquals(tx.ecn, b.ecn(0))
            }
        }
        assertEquals(expected, datagrams)
    }

    @Test
    fun basic() = runReactor {
        val send = bindLocal(); val recv = bindLocal()
        testSendRecv(send, recv, Tx(recv.localAddress, null, "hello".encodeToByteArray()))
        send.close(); recv.close()
    }

    @Test
    fun basicSrcIp() = runReactor {
        val send = bindLocal(); val recv = bindLocal()
        testSendRecv(send, recv, Tx(recv.localAddress, null, "hello".encodeToByteArray(), srcIp = send.localAddress))
        send.close(); recv.close()
    }

    @Test
    fun ecnV6() = runReactor {
        val send = bindUdp(SocketAddress.IPV6_LOCALHOST_ANY_PORT); val recv = bindUdp(SocketAddress.IPV6_LOCALHOST_ANY_PORT)
        for (cp in listOf(EcnCodepoint.Ect0, EcnCodepoint.Ect1)) testSendRecv(send, recv, Tx(recv.localAddress, cp, "hello".encodeToByteArray()))
        send.close(); recv.close()
    }

    @Test
    fun ecnV4() = runReactor {
        val send = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val recv = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        for (cp in listOf(EcnCodepoint.Ect0, EcnCodepoint.Ect1)) testSendRecv(send, recv, Tx(recv.localAddress, cp, "hello".encodeToByteArray()))
        send.close(); recv.close()
    }

    @Test
    fun ecnV6Dualstack() = runReactor {
        // The unspecified address, not a local one, for dual-stack mode (reference comment).
        val recv = bindUdp(SocketAddress.IPV6_UNSPECIFIED_ANY_PORT, UdpOptions(ipv6Only = false))
        val port = recv.localAddress.port
        val recvV6 = SocketAddress.of(SocketAddress.IPV6_LOCALHOST_ANY_PORT.ipBytes(), port)
        val recvV4 = SocketAddress.ipv4(127, 0, 0, 1, port)
        for ((src, dst) in listOf(SocketAddress.IPV6_LOCALHOST_ANY_PORT to recvV6, SocketAddress.IPV4_LOCALHOST_ANY_PORT to recvV4)) {
            val send = bindUdp(src)
            for (cp in listOf(EcnCodepoint.Ect0, EcnCodepoint.Ect1)) testSendRecv(send, recv, Tx(dst, cp, "hello".encodeToByteArray()))
            send.close()
        }
        recv.close()
    }

    @Test
    fun ecnV4MappedV6() = runReactor {
        val send = bindUdp(SocketAddress.IPV6_UNSPECIFIED_ANY_PORT, UdpOptions(ipv6Only = false))
        val recv = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        val mapped = SocketAddress.ipv4(127, 0, 0, 1, recv.localAddress.port).toIpv4Mapped()
        for (cp in listOf(EcnCodepoint.Ect0, EcnCodepoint.Ect1)) testSendRecv(send, recv, Tx(mapped, cp, "hello".encodeToByteArray()))
        send.close(); recv.close()
    }

    /**
     * Linux, Android and Windows in the reference (`#[cfg_attr(not(linux, windows, android), ignore)]`); elsewhere max
     * segments is 1. The segment counts are quinn-udp's: Linux 64 / 64 (UDP_SEGMENT, UDP_GRO), Apple 1 / 1, Windows
     * 512 with USO (else 1) / 64; Android's depend on its kernel. A send with segments must not fall back to one.
     */
    @Test
    fun gso() = runReactor {
        val send = bindLocal(); val recv = bindLocal()
        val counts = send.maxGsoSegments to recv.groSegments
        when (Platform.osFamily) {
            OsFamily.LINUX -> assertEquals(64 to 64, counts)
            OsFamily.MACOSX, OsFamily.IOS -> assertEquals(1 to 1, counts)
            OsFamily.WINDOWS -> { assertTrue(counts.first in setOf(1, 512), "$counts"); assertEquals(64, counts.second) }
            else -> assertTrue(counts.first in setOf(1, 64) && counts.second in setOf(1, 64), "$counts")
        }
        val segment = 128
        val msg = ByteArray(segment * send.maxGsoSegments) { 0xAB.toByte() }
        testSendRecv(send, recv, Tx(recv.localAddress, null, msg, segmentSize = segment))
        assertEquals(counts.first, send.maxGsoSegments, "the segmented send fell back to one segment")
        send.close(); recv.close()
    }

    @Test
    fun socketBuffers() = runReactor {
        val size = 123456
        // Linux / Android double the requested size
        val factor = if (Platform.osFamily == OsFamily.LINUX || Platform.osFamily == OsFamily.ANDROID) 2 else 1
        val send = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val recv = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        for (s in listOf(send, recv)) {
            val before = s.sendBufferSize()
            assertTrue(before != size * factor, "make sure buffer is not already desired size")
            s.setSendBufferSize(size)
            assertEquals(size * factor, s.sendBufferSize(), "send buffer $before -> ${s.sendBufferSize()}")
            s.setReceiveBufferSize(size)
            assertEquals(size * factor, s.receiveBufferSize())
        }
        testSendRecv(send, recv, Tx(recv.localAddress, null, "hello".encodeToByteArray()))
        send.close(); recv.close()
    }

    // ---- SPEC §29.5 additions -------------------------------------------------------------------

    /**
     * One recv call returns up to BATCH_SIZE datagrams: 32 by default (Linux / Android via recvmmsg; Apple and Windows
     * one recvmsg / WSARecvMsg each inside the shim, SPEC §29.9).
     */
    @Test
    fun batchReceive() = runReactor {
        val send = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val recv = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        Transmit(64).use { t ->
            t.setDestination(recv.localAddress)
            repeat(BATCH_SIZE) { i -> t.buffer[0] = i.toByte(); t.length = 1; assertTrue(send.trySend(t)) }
        }
        RecvBatch(BATCH_SIZE, 64).use { b ->
            var got = 0
            val seen = BooleanArray(BATCH_SIZE)
            while (got < BATCH_SIZE) {
                val n = recv.recv(b)
                for (i in 0 until n) { seen[b.buffer[b.offset(i)].toInt()] = true; assertEquals(1, b.length(i)) }
                // All were queued before the first call: it takes the whole batch (every driver in CI, 2026-10).
                if (got == 0) assertEquals(BATCH_SIZE, n, "the first call returned $n of $BATCH_SIZE")
                got += n
            }
            assertTrue(seen.all { it })
        }
        send.close(); recv.close()
    }

    /** EMSGSIZE (a datagram larger than the path allows, as with MTU probes) counts as sent (SPEC §29.3). */
    @Test
    fun oversizedDatagramIsNotAnError() = runReactor {
        val send = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT); val recv = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        Transmit(65535).use { t ->
            t.setDestination(recv.localAddress); t.length = 65535      // > 65507, the IPv4 UDP maximum
            send.send(t)
        }
        send.close(); recv.close()
    }

    /** close() wakes a parked recv with ClosedException. */
    @Test
    fun closeWakesParkedRecv() = runReactor {
        val s = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT)
        val parked = async { RecvBatch(1, 64).use { b -> runCatching { s.recv(b) }.exceptionOrNull() } }
        delay(50)
        s.close()
        val e = parked.await()
        assertTrue(e is ClosedException, "a parked recv ended with $e")
    }
}
