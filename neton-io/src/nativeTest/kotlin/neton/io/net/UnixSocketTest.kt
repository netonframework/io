package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** SPEC §23.6: Unix domain sockets. Relative paths keep well inside sun_path on every platform. */
class UnixSocketTest {

    private suspend fun roundTrip(c: IoStream, msg: String): String {
        val f = Framed(Io(c), LineCodec, LineCodec)
        f.send(msg)
        return f.incoming().first()
    }

    /**
     * Whether this environment lets us create socket files here. Android's SELinux policy forbids
     * the adb shell domain from creating them in /data/local/tmp (bind fails with EACCES); the
     * abstract namespace still works there.
     */
    private suspend fun socketFilesAllowed(test: String): Boolean {
        val probe = "neton-uds-probe.sock"
        return try { listenUnix(probe).close(); true } catch (e: IllegalStateException) {
            if (e.message?.contains("errno=13") != true) throw e
            println("SKIP $test: socket files are not allowed here (${e.message})"); false
        }
    }

    private suspend fun echoOnce(l: UnixListener) {
        val s = l.accept()
        try { val f = Framed(Io(s), LineCodec, LineCodec); f.send(f.incoming().first()) } finally { s.close() }
    }

    @Test
    fun echoOverASocketFile() = runReactor {
        if (!socketFilesAllowed("echoOverASocketFile")) return@runReactor
        val path = "neton-uds-echo.sock"
        val l = listenUnix(path)
        val server = launch { echoOnce(l) }
        val c = connectUnix(path)
        assertEquals("hello", roundTrip(c, "hello"))
        c.close(); server.join(); l.close()
        // close removes the file: nobody can connect any more.
        assertFailsWith<ConnectException> { connectUnix(path) }
    }

    @Test
    fun tooLongPathIsRejected() = runReactor {
        val path = "p".repeat(SUN_PATH_SIZE)
        assertFailsWith<IllegalArgumentException> { listenUnix(path) }
        assertFailsWith<IllegalArgumentException> { connectUnix(path) }
        if (socketFilesAllowed("tooLongPathIsRejected (bind part)")) listenUnix("q".repeat(90)).close()                        // long but within every platform's limit
    }

    @Test
    fun staleSocketFileIsReplacedButALiveOneIsNot() = runReactor {
        if (!socketFilesAllowed("staleSocketFileIsReplacedButALiveOneIsNot")) return@runReactor
        val path = "neton-uds-stale.sock"
        // A "crashed" listener: its fd is closed but the socket file stays behind.
        val crashed = listenUnixServer(path, SocketOptions.Default)
        crashed.afterClose = null
        crashed.close()
        val l = listenUnix(path)                                  // stale file: removed and rebound
        val e = runCatching { listenUnix(path) }.exceptionOrNull() // live listener: refused
        assertTrue(e is IllegalStateException, "binding over a live listener must fail, got $e")
        // The refused bind's probe reached the live listener: it is the first, empty, connection.
        val probe = l.accept()
        assertEquals(-1, probe.read(neton.io.bytes.Buffer()), "the probe connection closes without data")
        probe.close()
        val server = launch { echoOnce(l) }
        val c = connectUnix(path)
        assertEquals("still mine", roundTrip(c, "still mine"))
        c.close(); server.join(); l.close()
    }

    @Test
    fun abstractNamespaceOnLinuxOnly() = runReactor {
        if (!UNIX_ABSTRACT_SUPPORTED) {
            assertFailsWith<IllegalArgumentException> { listenUnix("@neton-uds-abstract") }
            return@runReactor
        }
        val l = listenUnix("@neton-uds-abstract")
        val server = launch { echoOnce(l) }
        val c = connectUnix("@neton-uds-abstract")
        assertEquals("abstract", roundTrip(c, "abstract"))
        c.close(); server.join(); l.close()
    }

    @Test
    fun groupServesOverAUnixSocket() = runReactor {
        if (!socketFilesAllowed("groupServesOverAUnixSocket")) return@runReactor
        val path = "neton-uds-group.sock"
        val g = listenUnixGroup(path, reactors = 2)
        val serveJob = launch {
            g.serve { conn ->
                try { val f = Framed(Io(conn), LineCodec, LineCodec); f.incoming().collect { f.send(it) } } finally { conn.close() }
            }
        }
        val clients = (0 until 16).map { i -> launch {
            val c = connectUnix(path)
            assertEquals("u$i", roundTrip(c, "u$i"))
            c.close()
        } }
        clients.forEach { it.join() }
        g.shutdown(1_000); serveJob.join(); g.awaitWorkers()
        assertFailsWith<ConnectException> { connectUnix(path) }
    }
}
