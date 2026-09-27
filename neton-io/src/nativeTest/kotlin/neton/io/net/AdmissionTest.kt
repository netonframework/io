package neton.io.net

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.codec.LineCodec
import neton.io.core.Admission
import neton.io.core.AdmissionTimeoutException
import neton.io.core.FrameReadRate
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.io.core.Service
import neton.io.core.serve
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/** SPEC §28.12: admission before reading. Ports below 32768 (see FairnessTest). */
class AdmissionTest {

    private class Server(val admission: Admission, val job: Job, val framed: MutableList<Framed<String, String>>, val errors: MutableList<Throwable>)

    /** A line echo server whose connections are served with [admission]. */
    private suspend fun CoroutineScope.server(
        port: Int, admission: Admission, rate: FrameReadRate = FrameReadRate(200, 1_000, 1),
        handler: suspend (String) -> String = { it },
    ): Server {
        val l = listen("127.0.0.1", port)
        val framed = ArrayList<Framed<String, String>>(); val errors = ArrayList<Throwable>()
        val job = launch {
            try {
                while (true) {
                    val conn = l.accept()
                    launch {
                        val f = Framed(Io(conn), LineCodec, LineCodec, readRate = rate)
                        framed.add(f)
                        try { serve(f, Service { handler(it) }, admission) } catch (e: IoException) { errors.add(e) } finally { conn.close() }
                    }
                }
            } finally { l.close() }
        }
        return Server(admission, job, framed, errors)
    }

    private suspend fun send(c: IoStream, s: String) { c.write(Buffer().also { it.writeBytes(s.encodeToByteArray()) }) }

    private suspend fun readLine(c: IoStream): String? {
        val acc = StringBuilder()
        while (true) {
            val b = Buffer()
            if (c.read(b) < 0) return null
            acc.append(b.readAll().decodeToString())
            if (acc.endsWith("\n")) return acc.toString().trimEnd('\n')
        }
    }

    /** Read until EOF or error; true if the peer closed. */
    private suspend fun closedByPeer(c: IoStream): Boolean =
        try { while (c.read(Buffer()) >= 0) { }; true } catch (e: IoException) { true }

    @Test
    fun adminssionWithoutReadRateIsRejected() = runReactor {
        val (a, b) = neton.io.core.memoryStreamPair()
        val f = Framed(Io(a), LineCodec, LineCodec)
        assertFailsWith<IllegalArgumentException> { f.serveLoop(Admission(1, 100)) { it } }
        a.close(); b.close()
    }

    /** (a) Idle connections hold no permit. */
    @Test
    fun idleConnectionsHoldNoPermit() = runReactor {
        val s = server(21801, Admission(2, 1_000))
        val idle = List(2) { connect("127.0.0.1", 21801) }
        delay(100)
        val c = connect("127.0.0.1", 21801)
        send(c, "real\n")
        assertEquals("real", withTimeout(1_000) { readLine(c) })
        assertEquals(0, s.admission.inUse)
        (idle + c).forEach { it.close() }; s.job.cancelAndJoin()
    }

    /** (b) Connections that stop after one byte lose their permit at the frame read rate deadline. */
    @Test
    fun stalledPermitHoldersAreClosedByTheReadRate() = runReactor {
        val s = server(21802, Admission(2, 5_000), rate = FrameReadRate(200, 400, 1))
        val stalled = List(2) { connect("127.0.0.1", 21802).also { send(it, "x") } }
        delay(50)
        assertEquals(2, s.admission.inUse, "each stalled connection holds a permit")
        val t0 = TimeSource.Monotonic.markNow()
        val c = connect("127.0.0.1", 21802)
        send(c, "real\n")
        assertEquals("real", withTimeout(2_000) { readLine(c) })
        val waited = t0.elapsedNow().inWholeMilliseconds
        assertTrue(waited >= 100, "served after $waited ms: it should have waited for a permit")
        stalled.forEach { assertTrue(withTimeout(2_000) { closedByPeer(it) }) }
        (stalled + c).forEach { it.close() }; s.job.cancelAndJoin()
    }

    /** (c) Many pipelining connections: never more than `permits` requests in progress; buffers stay bounded. */
    @Test
    fun inProgressRequestsNeverExceedPermits() = runReactor {
        var now = 0; var peak = 0
        val s = server(21803, Admission(2, 10_000)) { req -> now++; if (now > peak) peak = now; delay(1); now--; req }
        val n = 10; val perConn = 200
        val clients = List(n) { connect("127.0.0.1", 21803) }
        val readers = clients.map { c ->
            launch {
                var got = 0; val acc = StringBuilder()
                while (got < perConn) { val b = Buffer(); if (c.read(b) < 0) break; acc.append(b.readAll().decodeToString()); got = acc.count { it == '\n' } }
                assertEquals(perConn, got)
            }
        }
        clients.forEach { c -> launch { send(c, (0 until perConn).joinToString("") { "req-$it\n" }) } }
        withTimeout(20_000) { readers.forEach { it.join() } }
        assertTrue(peak <= 2, "peak in-progress requests $peak > permits")
        val maxCap = s.framed.maxOf { it.io.readBuf.capacity }
        assertTrue(maxCap <= 64 * 1024, "a server read buffer grew to $maxCap bytes")
        assertEquals(0, s.admission.inUse)
        clients.forEach { it.close() }; s.job.cancelAndJoin()
    }

    /** (d) Waiting longer than acquireTimeoutMillis closes the connection and is counted. */
    @Test
    fun waitingPastTheAcquireTimeoutClosesTheConnection() = runReactor {
        val s = server(21804, Admission(1, 100), rate = FrameReadRate(3_000, 3_000, 1))
        val holder = connect("127.0.0.1", 21804).also { send(it, "x") }
        delay(50)
        val c = connect("127.0.0.1", 21804)
        send(c, "real\n")
        assertTrue(withTimeout(2_000) { closedByPeer(c) })
        assertEquals(1L, s.admission.timeouts)
        assertTrue(s.errors.any { it is AdmissionTimeoutException })
        // The wait statistics saw that one contended acquire, and it lasted at least the limit.
        assertEquals(1L, s.admission.waits)
        assertTrue(s.admission.waitQuantileMicros(1.0) >= 100_000, "max wait ${s.admission.waitQuantileMicros(1.0)} µs")
        holder.close(); c.close(); s.job.cancelAndJoin()
    }

    /** (e) A connection cancelled while waiting for a permit leaks nothing. */
    @Test
    fun cancelledWaiterLeaksNoPermit() = runReactor {
        val s = server(21805, Admission(1, 10_000), rate = FrameReadRate(3_000, 3_000, 1))
        val holder = connect("127.0.0.1", 21805).also { send(it, "x") }
        delay(50)
        val waiter = connect("127.0.0.1", 21805).also { send(it, "real\n") }
        delay(50)
        s.job.cancelAndJoin()                       // cancels the holder's and the waiter's server coroutines
        assertEquals(0, s.admission.inUse, "a permit leaked")
        holder.close(); waiter.close()
    }

    /** (f) A client that never reads responses: the server's pending output per connection stays bounded. */
    @Test
    fun pendingOutputStaysBoundedWhenTheClientDoesNotRead() = runReactor {
        var peakPending = 0
        lateinit var srv: Server
        val big = "y".repeat(1024)
        srv = server(21806, Admission(4, 10_000)) { _ ->
            srv.framed.forEach { if (it.io.writeBuf.readableBytes > peakPending) peakPending = it.io.writeBuf.readableBytes }
            big
        }
        val c = connect("127.0.0.1", 21806)
        val writer = launch { try { while (true) send(c, "q\n".repeat(512)) } catch (_: IoException) { } }
        delay(1_000)
        assertTrue(peakPending <= 64 * 1024 + big.length + 1, "pending output reached $peakPending bytes")
        writer.cancelAndJoin(); c.close(); srv.job.cancelAndJoin()
    }
}
