package neton.io.net

import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A backlog of connections that completed their handshake is taken in a few turns of the reactor loop,
 * not one connection per turn. Found on io_uring (SPEC §32): each accept submitted an ACCEPT SQE and
 * waited for the next loop iteration, so under load a burst of 4096 connections was accepted at about
 * 100 per second and most of them waited seconds with their first request unread. The readiness
 * reactors always accepted until EAGAIN. Measured in the turns of a coroutine that only yields, not in time:
 * each loop iteration runs it up to the task budget (256) times, so one accept per iteration (the defect)
 * costs about 256 turns per connection (76,498 for 300 on io_uring before the fix), a direct accept almost none.
 */
class AcceptBacklogTest {

    @Test
    fun aQueuedBacklogIsAcceptedInAFewLoopTurns() = runReactor {
        // Within every platform's default listen queue cap (macOS kern.ipc.somaxconn is 128).
        val backlog = 100
        val server = listen("127.0.0.1", 21975)
        // Every client completes its handshake; the kernel queues the connections until they are accepted.
        val clients = ArrayList<IoStream>(backlog)
        repeat(backlog) { clients += connect("127.0.0.1", 21975) }

        var turns = 0
        var accepting = true
        val ticker = launch {
            while (accepting) { turns++; yield() }
        }
        yield()                                       // the ticker is running
        val before = turns
        val accepted = ArrayList<IoStream>(backlog)
        repeat(backlog) { accepted += server.accept() }
        val used = turns - before
        accepting = false
        ticker.join()

        assertEquals(backlog, accepted.size)
        // Fewer turns than one loop iteration's budget: the queue was drained without waiting on the loop.
        assertTrue(used < backlog / 10, "accepting $backlog queued connections took $used loop turns")
        accepted.forEach { it.close() }
        clients.forEach { it.close() }
        server.close()
    }
}
