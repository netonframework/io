package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import neton.io.core.IoStream
import kotlin.experimental.ExperimentalNativeApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/** SPEC §23.5: every option is read back from the socket with getsockopt. */
@OptIn(ExperimentalNativeApi::class)
class SocketOptionsTest {

    private val isWindows = Platform.osFamily == OsFamily.WINDOWS

    private fun fdOf(s: IoStream): Int = (s as ReactorStream).fd

    @Test
    fun connectOptionsAreApplied() = runReactor {
        val server = listen("127.0.0.1", 21880)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(server.accept()) }
        val opts = SocketOptions(keepAlive = KeepAlive(30, 10, 4), sendBufferSize = 65536, receiveBufferSize = 65536)
        val c = connect("127.0.0.1", 21880, opts)
        val snap = readSocketOptions(fdOf(c))
        assertTrue(snap.noDelay, "noDelay")
        assertTrue(snap.keepAlive, "keepAlive")
        assertEquals(30, snap.keepIdleSeconds, "keepalive idle")
        assertEquals(10, snap.keepIntervalSeconds, "keepalive interval")
        assertEquals(4, snap.keepProbes, "keepalive probes")
        // Linux reports twice the requested size (bookkeeping overhead); others report it as set.
        assertTrue(snap.sendBufferSize >= 65536, "SO_SNDBUF ${snap.sendBufferSize}")
        assertTrue(snap.receiveBufferSize >= 65536, "SO_RCVBUF ${snap.receiveBufferSize}")
        accepted.await().close(); c.close(); server.close()
    }

    @Test
    fun listenerOptionsApplyToAcceptedConnections() = runReactor {
        val server = listen("127.0.0.1", 21881, SocketOptions(keepAlive = KeepAlive(20, 5, 3)))
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(server.accept()) }
        val c = connect("127.0.0.1", 21881)
        val snap = readSocketOptions(fdOf(accepted.await()))
        assertTrue(snap.keepAlive, "accepted keepAlive")
        assertEquals(20, snap.keepIdleSeconds, "accepted keepalive idle")
        assertTrue(snap.noDelay, "accepted noDelay (default)")
        accepted.await().close(); c.close(); server.close()
    }

    @Test
    fun noDelayCanBeTurnedOff() = runReactor {
        val server = listen("127.0.0.1", 21882)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(server.accept()) }
        val c = connect("127.0.0.1", 21882, SocketOptions(noDelay = false))
        assertEquals(false, readSocketOptions(fdOf(c)).noDelay)
        accepted.await().close(); c.close(); server.close()
    }

    /** POSIX only: Windows ignores reusePort (SPEC §23.5). */
    @Test
    fun reusePortAllowsTwoListenersOnOnePort() = runReactor {
        if (isWindows) { println("SKIP reusePort: not supported on Windows"); return@runReactor }
        val opts = SocketOptions(reusePort = true)
        val a = listen("127.0.0.1", 21883, opts)
        val b = listen("127.0.0.1", 21883, opts)       // would fail with EADDRINUSE without SO_REUSEPORT
        a.close(); b.close()
        val c = listen("127.0.0.1", 21884)
        assertFailsWith<IllegalStateException> { listen("127.0.0.1", 21884) }
        c.close()
    }

    /**
     * 192.0.2.1 (TEST-NET-1, RFC 5737) is never routed: the SYN goes unanswered, so only the
     * timeout ends the attempt (or the network reports it unreachable at once — also a
     * ConnectException). A transparent proxy that completes every handshake locally (Clash TUN on
     * the development Mac) makes the premise false; the test says so and stops.
     */
    @Test
    fun connectTimeoutEndsAnUnansweredAttempt() = runReactor {
        val start = TimeSource.Monotonic.markNow()
        val r = runCatching { connect("192.0.2.1", 9, SocketOptions(connectTimeoutMillis = 300)) }
        val elapsed = start.elapsedNow().inWholeMilliseconds
        r.getOrNull()?.let {
            it.close()
            println("SKIP connectTimeout: 192.0.2.1 accepted a connection (transparent proxy on this network)")
            return@runReactor
        }
        val e = r.exceptionOrNull()
        assertTrue(e is ConnectException, "expected ConnectException, got $e")
        assertTrue(elapsed < 3_000, "connect attempt took $elapsed ms with a 300 ms timeout")
    }
}
