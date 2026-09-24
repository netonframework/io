package neton.io.net

import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Test ports are deliberately below 32768, outside the kernel's ephemeral range
// (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999). A fixed listen port
// inside that range intermittently loses the bind to some other process's outbound
// connection, which SO_REUSEADDR does not help with — it surfaces as a flaky EADDRINUSE.
/**
 * In-flight op lifetime: cancelling a coroutine parked in read while the peer later writes, and
 * leaving the reactor with a read still in flight. On io_uring these exercise the
 * pin-until-CQE / ASYNC_CANCEL / drain-on-shutdown paths; on readiness drivers they verify the
 * same contract (no crash, reactor exits, later traffic on the connection is intact).
 */
class InFlightLifetimeTest {

    @Test
    fun cancelParkedReadThenPeerWrites() = runReactor {
        val port = 19780
        val server = listen("127.0.0.1", port)
        var serverConn: neton.io.core.IoStream? = null
        val accepted = launch { serverConn = server.accept() }
        val client = connect("127.0.0.1", port)
        accepted.join()

        // Park a read on a buffer, cancel it, then have the peer write into that connection.
        val parkedBuf = Buffer(4096)
        val reader = launch { client.read(parkedBuf) }
        yield() // let the read submit and park
        reader.cancelAndJoin()

        val out = Buffer()
        out.writeBytes("after-cancel".encodeToByteArray())
        serverConn!!.write(out)

        // A fresh read on the same connection sees the data (or the cancelled op consumed it —
        // either way the process is intact and the bytes are not lost to a freed buffer).
        val fresh = Buffer(4096)
        var got = ""
        var attempts = 0
        while (got.isEmpty() && attempts < 50) {
            fresh.clear()
            val n = client.read(fresh)
            if (n < 0) break
            got = fresh.readAll().decodeToString()
            attempts++
        }
        assertTrue(got == "after-cancel" || parkedBuf.readableBytes > 0, "data neither in fresh read nor in cancelled buffer")

        client.close()
        serverConn!!.close()
        server.close()
    }

    @Test
    fun reactorExitsWithCancelledReadStillInFlight() {
        // Cancelling the awaiter completes the coroutine immediately, but on a completion driver
        // the kernel op is still in flight; the reactor must drain it on shutdown, not close the
        // ring under it. (runReactor only returns once the root and its children are complete, so
        // an un-cancelled read can never be "left behind" — cancellation is the only way in.)
        var finished = false
        runReactor {
            val port = 19781
            val server = listen("127.0.0.1", port)
            var serverConn: neton.io.core.IoStream? = null
            val accepted = launch { serverConn = server.accept() }
            val client = connect("127.0.0.1", port)
            accepted.join()
            val reader = launch { client.read(Buffer(1024)) }
            yield() // read submitted and parked
            reader.cancel() // no join, no close: the op may still be in flight when the root ends
            server.close()
            finished = true
        }
        assertEquals(true, finished)
    }
}
