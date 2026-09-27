package neton.io.net

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import kotlin.concurrent.AtomicInt
import kotlin.concurrent.AtomicReference
import kotlin.coroutines.coroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * SPEC §27.9: a connection coroutine gives back what it owns exactly once, on its own reactor,
 * however it ends — including cancelled before it ever started.
 */
class ConnectionReclaimTest {
    /** A set that fails any change made off its owner thread (checked on the reactor's own id). */
    private class OwnedSet(private val owner: ULong) : MutableSet<Job> by HashSet() {
        private val inner = HashSet<Job>()
        val violations = AtomicInt(0)
        private fun check() { if (currentThreadId() != owner) violations.incrementAndGet() }
        override fun add(element: Job): Boolean { check(); return inner.add(element) }
        override fun remove(element: Job): Boolean { check(); return inner.remove(element) }
        override val size: Int get() = inner.size
        override fun isEmpty(): Boolean = inner.isEmpty()
    }

    /** One accepted server-side fd plus its client end. */
    private suspend fun acceptedPair(server: TcpServer, port: Int): Pair<Int, IoStream> {
        val client = connect("127.0.0.1", port)
        return server.acceptFd() to client
    }

    private suspend fun peerSeesEof(client: IoStream) {
        assertEquals(-1, withTimeout(2_000) { client.read(Buffer()) }, "the server side fd must be closed")
        client.close()
    }

    @Test
    fun parentScopeAlreadyCancelled() = runReactor {
        val server = listenTcpServer("127.0.0.1", 21980, SocketOptions.Default)
        val (fd, client) = acceptedPair(server, 21980)
        val dead = CoroutineScope(coroutineContext + Job().also { it.cancel() })
        var ran = false; var released = 0
        val jobs = OwnedSet(currentThreadId())
        val job = startConnection(dead, currentReactor(), fd, jobs, { released++ }) { ran = true }
        withTimeout(2_000) { job.join() }
        assertTrue(!ran, "the handler must not run"); assertEquals(1, released); assertTrue(jobs.isEmpty())
        assertEquals(0, jobs.violations.value)
        peerSeesEof(client)
        server.close()
    }

    @Test
    fun cancelledFromAnotherThreadBeforeItStarts() = runReactor {
        val g = listenGroup("127.0.0.1", 21981, reactors = 2)
        val server = listenTcpServer("127.0.0.1", 21982, SocketOptions.Default)
        val (fd, client) = acceptedPair(server, 21982)
        val (scope1, reactor1) = g.memberForTest(1)
        val owner = AtomicReference<ULong>(0uL)
        g.callOnForTest(1) { owner.value = currentThreadId() }
        val jobs = OwnedSet(owner.value)
        val ran = AtomicInt(0); val released = AtomicInt(0)
        val jobRef = AtomicReference<Job?>(null); val cancelled = AtomicInt(0)
        g.runOnForTest(1) {
            jobRef.value = startConnection(scope1, reactor1, fd, jobs, { released.incrementAndGet() }) { ran.incrementAndGet() }
            // Hold reactor 1: the connection coroutine cannot start until the other thread cancelled it.
            while (cancelled.value == 0) platform.posix.usleep(100u)
        }
        withTimeout(2_000) { while (jobRef.value == null) delay(1) }
        jobRef.value!!.cancel(); cancelled.value = 1                     // from reactor 0's thread
        withTimeout(2_000) { while (released.value == 0) delay(1) }
        delay(50)
        assertEquals(0, ran.value, "the handler must not run"); assertEquals(1, released.value, "released exactly once")
        var empty = false; g.callOnForTest(1) { empty = jobs.isEmpty() }
        assertTrue(empty); assertEquals(0, jobs.violations.value, "the set was changed off its reactor")
        peerSeesEof(client)
        server.close(); g.shutdown(1_000); g.awaitWorkers()
    }

    @Test
    fun cancelRacingStart() = runReactor {
        val n = 300
        val g = listenGroup("127.0.0.1", 21983, reactors = 2)
        val server = listenTcpServer("127.0.0.1", 21984, SocketOptions.Default)
        val (scope1, reactor1) = g.memberForTest(1)
        val owner = AtomicReference<ULong>(0uL)
        g.callOnForTest(1) { owner.value = currentThreadId() }
        val jobs = OwnedSet(owner.value)
        val ran = AtomicInt(0); val released = AtomicInt(0)
        val clients = ArrayList<IoStream>()
        repeat(n) { k ->
            val (fd, client) = acceptedPair(server, 21984)
            clients.add(client)
            val jobRef = AtomicReference<Job?>(null)
            g.runOnForTest(1) {
                jobRef.value = startConnection(scope1, reactor1, fd, jobs, { released.incrementAndGet() }) { conn ->
                    ran.incrementAndGet(); conn.read(Buffer())                  // parks until cancelled
                }
            }
            while (jobRef.value == null) yield()
            repeat(k % 4) { yield() }                                           // vary the cancel point
            jobRef.value!!.cancel()
        }
        runCatching { withTimeout(10_000) { while (released.value < n) delay(5) } }.onFailure { println("ConnectionReclaimTest.cancelRacingStart: released ${released.value} of $n, handler ran ${ran.value}") }
        delay(50)
        assertEquals(n, released.value, "every connection released exactly once")
        var empty = false; g.callOnForTest(1) { empty = jobs.isEmpty() }
        assertTrue(empty); assertEquals(0, jobs.violations.value)
        println("ConnectionReclaimTest.cancelRacingStart: handler ran in ${ran.value} of $n")
        for (c in clients) c.close()
        server.close(); withTimeout(5_000) { g.shutdown(1_000); g.awaitWorkers() }
    }
}
