package neton.io.net

import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import neton.io.core.IoStream
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Failure paths at the socket boundary: a refused connect and a peer that goes away mid-write. */
class TcpFailureTest {

    @Test
    fun connectRefusedThrows() = runReactor {
        // Nothing listens here; a non-blocking connect completes with SO_ERROR=ECONNREFUSED.
        val ex = assertFailsWith<ConnectException> { connect("127.0.0.1", 39777) }
        assertTrue(ex.message!!.contains("39777"), ex.message)
    }

    @Test
    fun writeToClosedPeerThrowsIoException() = runReactor {
        val port = 39778
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
        val port = 39779
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
        val port = 39782
        val server = listen("127.0.0.1", port)
        val accepted = launch { server.accept().close() }
        val client = connect("127.0.0.1", port)
        accepted.join()
        val worker = Worker.start()
        val result = worker.execute(TransferMode.SAFE, { client }) { c ->
            try { c.close(); "closed" } catch (t: IllegalStateException) { "rejected" }
        }.result
        worker.requestTermination().result
        assertEquals("rejected", result)
        client.close()
        server.close()
    }
}
