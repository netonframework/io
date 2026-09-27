package neton.io.testkit

import neton.io.core.BaseFilter
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.SocketOptions
import neton.io.net.connect
import neton.io.net.connectUnix
import neton.io.net.listen
import neton.io.net.listenUnix
import neton.io.net.runReactor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * SPEC §28.6 / §28.11 step 2: every IoStream implementation in neton-io passes the conformance suite.
 * The socket runs use whichever driver NETON_IO_DRIVER / the platform selects; CI and the Linux
 * acceptance run repeat them per driver.
 */
class ConformanceTest {

    private fun assertConforms(failures: List<String>) {
        failures.forEach { println("FAIL $it") }
        assertEquals(emptyList(), failures)
    }

    private var nextPort = 22400

    private suspend fun tcpPair(serverOptions: SocketOptions = SocketOptions.Default): StreamPair {
        val port = nextPort++
        val l = listen("127.0.0.1", port, serverOptions)
        val a = connect("127.0.0.1", port)
        val b = l.accept()
        l.close()
        return StreamPair(a, b)
    }

    private var nextPath = 0

    private suspend fun unixPair(): StreamPair {
        val path = "neton-conf-${nextPath++}.sock"
        val l = listenUnix(path)
        val a = connectUnix(path)
        val b = l.accept()
        return StreamPair(a, b) { l.close() }
    }

    private fun memoryPair(): StreamPair { val (a, b) = memoryStreamPair(); return StreamPair(a, b) }

    private fun filtered(p: StreamPair): StreamPair = StreamPair(BaseFilter(p.a), p.b) { p.dispose() }

    @Test
    fun tcp() = runReactor {
        // The accepted end lingers 0 s, so its close sends RST.
        assertConforms(IoStreamConformance("tcp", { tcpPair() }, { tcpPair(SocketOptions(lingerSeconds = 0)) }).run())
    }

    @Test
    fun unixSocket() = runReactor {
        val probe = runCatching { listenUnix("neton-conf-probe.sock").close() }
        if (probe.isFailure) { println("SKIP unixSocket: ${probe.exceptionOrNull()}"); return@runReactor }
        assertConforms(IoStreamConformance("unix", { unixPair() }).run())
    }

    @Test
    fun memoryStream() = runReactor {
        assertConforms(IoStreamConformance("memory", { memoryPair() }).run())
    }

    @Test
    fun baseFilterOverTcp() = runReactor {
        assertConforms(IoStreamConformance("filter(tcp)", { filtered(tcpPair()) }).run())
    }

    @Test
    fun baseFilterOverMemory() = runReactor {
        assertConforms(IoStreamConformance("filter(memory)", { filtered(memoryPair()) }).run())
    }

    /** The suite itself must catch a stream that breaks the contract. */
    @Test
    fun suiteCatchesAViolation() = runReactor {
        class ZeroRead(inner: IoStream) : IoStream by inner {
            override suspend fun read(dst: neton.io.bytes.Buffer): Int = 0
        }
        val failures = IoStreamConformance("broken", { val (a, b) = memoryStreamPair(); StreamPair(ZeroRead(a), b) }, timeoutMillis = 1_000).run()
        assertTrue(failures.any { "read appends" in it }, "the suite missed read() == 0: $failures")
    }
}
