package neton.io.net

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** [lookupHost]: literals inline, names through the resolver, the port carried into every address. */
class LookupHostTest {
    @Test
    fun ipv4Literal() = runReactor {
        val a = lookupHost("127.0.0.1", 4433).single()
        assertTrue(a.isIpv4)
        assertContentEquals(byteArrayOf(127, 0, 0, 1), a.ipBytes())
        assertEquals(4433, a.port)
    }

    @Test
    fun ipv6LiteralBareAndBracketed() = runReactor {
        for (host in listOf("::1", "[::1]")) {
            val a = lookupHost(host, 65535).single()
            assertTrue(a.isIpv6, host)
            assertContentEquals(ByteArray(16).also { it[15] = 1 }, a.ipBytes(), host)
            assertEquals(65535, a.port, host)
        }
    }

    @Test
    fun localhostResolvesToLoopback() = runReactor {
        val addrs = lookupHost("localhost", 80)
        assertTrue(addrs.isNotEmpty())
        for (a in addrs) {
            assertEquals(80, a.port)
            val ip = a.toCanonical().ipBytes()
            assertTrue(if (ip.size == 4) ip[0] == 127.toByte() else ip.contentEquals(ByteArray(16).also { it[15] = 1 }), "$ip")
        }
    }

    /** A name that does not resolve (unless a fake-IP proxy answers everything, see AddressTest). */
    @Test
    fun unknownHost() = runReactor {
        val host = "no-such-host.invalid"
        val addrs = try { lookupHost(host, 1) } catch (e: UnknownHostException) {
            assertEquals(host, e.host)
            return@runReactor
        }
        assertTrue(addrs.all { it.isIpv4 && it.ipBytes()[0] == 198.toByte() }, "resolved to $addrs")
        println("SKIP unknownHost: resolver is a fake-IP proxy")
    }

    @Test
    fun portOutOfRange() = runReactor {
        assertFailsWith<IllegalArgumentException> { lookupHost("127.0.0.1", 65536) }
    }
}
