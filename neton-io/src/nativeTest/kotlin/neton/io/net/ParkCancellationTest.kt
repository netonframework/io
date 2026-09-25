package neton.io.net

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import kotlin.coroutines.coroutineContext
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * SPEC §19.3: parks no longer go through CancellableContinuation, so cancellation is delivered by
 * a per-stream Job handler instead. These pin the contract down: a parked read wakes on
 * cancellation from the same thread and from another thread, an already-cancelled coroutine does
 * not park at all, and one coroutine parking many times on the same stream keeps working.
 */
@OptIn(ObsoleteWorkersApi::class)
class ParkCancellationTest {

    @Test
    fun parkedReadWakesOnCancelFromTheReactorThread() = runReactor {
        val server = listen("127.0.0.1", 39860)
        val accepted = CompletableDeferred<neton.io.core.IoStream>()
        launch { accepted.complete(server.accept()) }
        val client = connect("127.0.0.1", 39860)
        val conn = accepted.await()
        var outcome: Throwable? = null
        val reader = launch { try { conn.read(Buffer(16)) } catch (t: Throwable) { outcome = t; throw t } }
        delay(20)                                   // let it park
        withTimeout(2_000) { reader.cancelAndJoin() }
        assertTrue(outcome is CancellationException, "expected CancellationException, got $outcome")
        conn.close(); client.close(); server.close()
    }

    @Test
    fun parkedReadWakesOnCancelFromAnotherThread() = runReactor {
        val server = listen("127.0.0.1", 39861)
        val accepted = CompletableDeferred<neton.io.core.IoStream>()
        launch { accepted.complete(server.accept()) }
        val client = connect("127.0.0.1", 39861)
        val conn = accepted.await()
        var outcome: Throwable? = null
        val reader = launch { try { conn.read(Buffer(16)) } catch (t: Throwable) { outcome = t; throw t } }
        delay(20)
        val w = Worker.start(name = "canceller")
        w.execute(TransferMode.SAFE, { reader }) { it.cancel() }
        withTimeout(2_000) { reader.join() }
        w.requestTermination().result
        assertTrue(outcome is CancellationException, "expected CancellationException, got $outcome")
        conn.close(); client.close(); server.close()
    }

    @Test
    fun alreadyCancelledCoroutineDoesNotPark() = runReactor {
        val server = listen("127.0.0.1", 39862)
        val accepted = CompletableDeferred<neton.io.core.IoStream>()
        launch { accepted.complete(server.accept()) }
        val client = connect("127.0.0.1", 39862)
        val conn = accepted.await()
        var outcome: Throwable? = null
        val job = launch(start = CoroutineStart.UNDISPATCHED) {
            coroutineContext.job.cancel()           // cancelled, but still running this body
            try { conn.read(Buffer(16)) } catch (t: Throwable) { outcome = t }
        }
        withTimeout(2_000) { job.join() }            // would hang if the read had parked
        assertTrue(outcome is CancellationException, "expected CancellationException, got $outcome")
        conn.close(); client.close(); server.close()
    }

    /** One coroutine, one stream, many parks: the reused cancellation handle must not get in the way. */
    @Test
    fun manyParksBySameCoroutineThenCancel() = runReactor {
        val server = listen("127.0.0.1", 39863)
        val accepted = CompletableDeferred<neton.io.core.IoStream>()
        launch { accepted.complete(server.accept()) }
        val client = connect("127.0.0.1", 39863)
        val conn = accepted.await()
        var echoed = 0
        val echo = launch {
            val buf = Buffer(64)
            while (true) { buf.clear(); if (conn.read(buf) < 0) break; conn.write(buf); echoed++ }
        }
        val out = Buffer(8); val back = Buffer(8)
        repeat(500) {
            out.clear(); out.writeBytes(byteArrayOf(1)); client.write(out)
            back.clear(); while (back.readableBytes < 1) client.read(back)
        }
        assertTrue(echoed >= 1, "echo coroutine never ran")
        withTimeout(2_000) { echo.cancelAndJoin() }   // parked again after the last echo
        assertEquals(true, echo.isCancelled)
        conn.close(); client.close(); server.close()
    }
}
