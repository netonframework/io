package neton.io.net

import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.serve
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// Test ports are deliberately below 32768, outside the kernel's ephemeral range
// (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999). A fixed listen port
// inside that range intermittently loses the bind to some other process's outbound
// connection, which SO_REUSEADDR does not help with — it surfaces as a flaky EADDRINUSE.
/** Failure paths at the socket boundary: a refused connect and a peer that goes away mid-write. */
class TcpFailureTest {

    @Test
    fun connectRefusedThrows() = runReactor {
        // Nothing listens here; a non-blocking connect completes with SO_ERROR=ECONNREFUSED.
        val ex = assertFailsWith<ConnectException> { connect("127.0.0.1", 19777) }
        assertTrue(ex.message!!.contains("19777"), ex.message)
    }

    @Test
    fun writeToClosedPeerThrowsIoException() = runReactor {
        val port = 19778
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            val conn = server.accept()
            conn.close() // hang up immediately
        }
        val client = connect("127.0.0.1", port)
        serverJob.join()

        // Keep writing until the kernel reports the reset. Without SIGPIPE suppressed this test
        // would not fail — the whole test process would be killed.
        val buf = Buffer(64 * 1024)
        var rounds = 0
        val failed = try {
            while (rounds < 1000) {
                buf.clear()
                buf.reserve(64 * 1024)
                buf.commitWrite(64 * 1024)
                client.write(buf)
                rounds++
            }
            false
        } catch (e: IoException) {
            true
        }
        assertTrue(failed, "write never reported the reset after $rounds rounds")
        client.close()
        server.close()
    }

    @Test
    fun closeWakesParkedReadWithClosedException() = runReactor {
        val port = 19779
        val server = listen("127.0.0.1", port)
        var serverConn: IoStream? = null
        val accepted = launch { serverConn = server.accept() }
        val client = connect("127.0.0.1", port)
        accepted.join()

        var outcome: Throwable? = null
        val reader = launch {
            try { client.read(Buffer(1024)) } catch (t: Throwable) { outcome = t }
        }
        yield() // read parked
        client.close()
        reader.join() // must not hang: close resumes the parked read
        assertTrue(outcome is ClosedException, "expected ClosedException, got $outcome")
        assertFailsWith<ClosedException> { client.read(Buffer(16)) } // use after close
        client.close() // idempotent
        serverConn!!.close()
        server.close()
    }

    @Test
    fun closeFromAnotherThreadIsRejected() = runReactor {
        val port = 19782
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            val conn = server.accept()
            serve(Framed(Io(conn), LineCodec, LineCodec)) { req -> "echo:$req" }
        }
        val client = connect("127.0.0.1", port)
        val framed = Framed(Io(client), LineCodec, LineCodec)

        val worker = startTestWorker()
        val result = worker.submit {
            try { client.close(); "closed" } catch (t: IllegalStateException) { "rejected" }
        }()
        worker.stop()

        // The server job is cancelled in `finally`: without it an assertion failure here would
        // leave `serve()` parked on the reactor and the test would hang instead of reporting.
        try {
            assertEquals("rejected", result)

            // P1-2: a rejected close must leave the stream exactly as it was. Before the fix
            // `closed` was set before the ownership check, so this round-trip failed with
            // ClosedException and the fd leaked — the rejection alone did not prove the invariant.
            framed.send("still-alive")
            assertEquals("echo:still-alive", framed.incoming().first())
            client.close()
        } finally {
            serverJob.cancelAndJoin()
            server.close()
        }
    }

    @Test
    fun closeListenerUnblocksAccept() = runReactor {
        val port = 19783
        val server = listen("127.0.0.1", port)
        var outcome = "still-parked"
        val acceptJob = launch {
            outcome = try {
                server.accept()
                "accepted"
            } catch (_: ClosedException) {
                "closed"
            }
        }
        // Let the accept reach the driver and park before the listener goes away.
        delay(50)

        server.close()
        acceptJob.join()
        assertEquals("closed", outcome)

        // Closing twice must be a no-op, not a second closeFd on a descriptor number the kernel
        // may already have reused.
        server.close()
    }
}
