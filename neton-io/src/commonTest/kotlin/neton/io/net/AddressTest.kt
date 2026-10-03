package neton.io.net

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** SPEC §18.2: names, IPv6 literals and dual-stack listening. */
class AddressTest {

    /** Accept one connection on [listener] and echo one line back. */
    private suspend fun echoOnce(listener: TcpListener) {
        val c = listener.accept()
        try {
            val f = Framed(Io(c), LineCodec, LineCodec)
            f.send(f.incoming().first())
        } finally { c.close() }
    }

    private suspend fun roundTrip(host: String, port: Int, line: String): String {
        val c = connect(host, port)
        try {
            val f = Framed(Io(c), LineCodec, LineCodec)
            f.send(line)
            return f.incoming().first()
        } finally { c.close() }
    }

    /** `localhost` may resolve to ::1 first; the IPv4-only listener must still be reached. */
    @Test
    fun connectByNameTriesEveryAddress() = runReactor {
        val server = listen("127.0.0.1", 21850)
        val srv = launch { echoOnce(server) }
        assertEquals("by-name", roundTrip("localhost", 21850, "by-name"))
        srv.join(); server.close()
    }

    @Test
    fun ipv6LoopbackEcho() = runReactor {
        val server = listen("::1", 21851)
        val srv = launch { echoOnce(server) }
        assertEquals("v6", roundTrip("::1", 21851, "v6"))
        srv.join(); server.close()
    }

    /** `::` listens dual-stack: an IPv4 client reaches it as a mapped address. */
    @Test
    fun dualStackListenerAcceptsIpv4() = runReactor {
        val server = listen("::", 21852)
        val srv = launch { echoOnce(server) }
        assertEquals("v4-on-v6", roundTrip("127.0.0.1", 21852, "v4-on-v6"))
        srv.join(); server.close()
    }

    /**
     * RFC 6761 reserves `.invalid`: it never resolves. That is a ConnectException, not a crash.
     *
     * Behind a fake-IP DNS proxy (Clash and similar, as on the development Mac) every name
     * "resolves" to 198.18.0.0/15, the RFC 2544 benchmarking range, and the proxy accepts the
     * connection — the premise does not hold there, so the test says so and stops. On a normal
     * resolver (153, CI) the error path is exercised.
     */
    @Test
    fun unresolvableNameIsAConnectException() = runReactor {
        val host = "no-such-host.invalid"
        val faked = try {
            resolve(host, 21853, passive = false).any { a ->
                // sockaddr_in keeps the IPv4 address at byte offset 4 on Linux and Apple alike.
                !a.isIpv6 && a.bytes.size >= 8 && (a.bytes[4].toInt() and 0xFF) == 198 && ((a.bytes[5].toInt() and 0xFF) shr 1) == 9
            }
        } catch (_: ResolveException) { false }
        if (faked) {
            println("SKIP unresolvableNameIsAConnectException: resolver is a fake-IP proxy ($host -> 198.18.0.0/15)")
            return@runReactor
        }
        val ex = assertFailsWith<ConnectException> { connect(host, 21853) }
        assertTrue(host in ex.message!!, ex.message)
    }
}
