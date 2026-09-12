package neton.io.net

import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
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
    fun writeToClosedPeerDoesNotKillProcess() = runReactor {
        val port = 39778
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            val conn = server.accept()
            conn.close() // hang up immediately
        }
        val client = connect("127.0.0.1", port)
        serverJob.join()

        // Keep writing until the kernel reports the reset. Without SIGPIPE ignored this test
        // would not fail — the whole test process would be killed.
        val buf = Buffer(64 * 1024)
        var rounds = 0
        var wrote = 0
        while (rounds < 1000) {
            buf.clear()
            buf.reserve(64 * 1024)
            buf.commitWrite(64 * 1024)
            val n = client.write(buf)
            rounds++
            if (n < 64 * 1024) break // the reactor reported an I/O error (EPIPE / ECONNRESET)
            wrote += n
        }
        assertTrue(rounds < 1000, "write never reported the reset (wrote $wrote bytes)")
        assertEquals(-1, client.read(Buffer()))
        client.close()
        server.close()
    }
}
