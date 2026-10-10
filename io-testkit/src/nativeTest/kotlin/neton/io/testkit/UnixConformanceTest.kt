package neton.io.testkit

import neton.io.net.connectUnix
import neton.io.net.listenUnix
import neton.io.net.runReactor
import kotlin.test.Test
import kotlin.test.assertEquals

/** The conformance suite on Unix domain sockets (native only; skipped where the platform has none). */
class UnixConformanceTest {
    private var nextPath = 0

    private suspend fun unixPair(): StreamPair {
        val path = "neton-conf-${nextPath++}.sock"
        val l = listenUnix(path)
        val a = connectUnix(path)
        val b = l.accept()
        return StreamPair(a, b) { l.close() }
    }

    @Test
    fun unixSocket() = runReactor {
        val probe = runCatching { listenUnix("neton-conf-probe.sock").close() }
        if (probe.isFailure) { println("SKIP unixSocket: ${probe.exceptionOrNull()}"); return@runReactor }
        val failures = IoStreamConformance("unix", { unixPair() }).run()
        failures.forEach { println("FAIL $it") }
        assertEquals(emptyList(), failures)
    }
}
