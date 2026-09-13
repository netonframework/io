package neton.io.net

import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.serve
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Test ports are deliberately below 32768, outside the kernel's ephemeral range
// (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999). A fixed listen port
// inside that range intermittently loses the bind to some other process's outbound
// connection, which SO_REUSEADDR does not help with — it surfaces as a flaky EADDRINUSE.
/**
 * Fairness: work that keeps re-dispatching itself must not starve I/O and timers. With an
 * unbounded task budget the loop would never reach the poller while a yield() loop is alive.
 */
class FairnessTest {

    @Test
    fun busyYieldLoopDoesNotStarveIoOrTimers() = runReactor {
        val port = 19795
        val server = listen("127.0.0.1", port)
        val serverJob = launch {
            while (true) {
                val conn = server.accept()
                launch { serve(Framed(Io(conn), LineCodec, LineCodec)) { it } }
            }
        }
        var spins = 0L
        val busy: Job = launch { while (true) { spins++; yield() } }

        // I/O must make progress next to the busy coroutine, within a bounded time.
        val client = connect("127.0.0.1", port)
        val framed = Framed(Io(client), LineCodec, LineCodec)
        val reply = withTimeout(5_000) {
            framed.send("ping")
            framed.incoming().first()
        }
        assertEquals("ping", reply)

        // Timers must fire too.
        withTimeout(5_000) { delay(20) }
        assertTrue(spins > 0)

        busy.cancelAndJoin()
        client.close()
        serverJob.cancelAndJoin()
        server.close()
    }
}
