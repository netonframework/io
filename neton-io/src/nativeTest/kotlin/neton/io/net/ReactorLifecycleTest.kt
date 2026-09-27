@file:OptIn(kotlin.native.concurrent.ObsoleteWorkersApi::class, kotlin.concurrent.atomics.ExperimentalAtomicApi::class, kotlinx.coroutines.InternalCoroutinesApi::class)

package neton.io.net

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import kotlin.concurrent.AtomicInt
import kotlin.concurrent.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §28.3: reactor lifecycle (DRAINING → CLOSING → STOPPED) and ReactorResumer accept / refuse. */
class ReactorLifecycleTest {

    /** (a) While draining, results from other threads, name lookups and cancellations still arrive. */
    @Test
    fun drainingDeliversWhatChildrenWaitFor() {
        val got = AtomicInt(-1); val resolved = AtomicInt(0); val cancelled = AtomicInt(0)
        val helper = Worker.start(name = "lifecycle-a")
        runReactor {
            val result = CompletableDeferred<Int>()
            launch { got.value = result.await() }
            launch { if (resolve("localhost", 80, passive = false).isNotEmpty()) resolved.value = 1 }
            val server = listenTcpServer("127.0.0.1", 21990, SocketOptions.Default)
            val client = connect("127.0.0.1", 21990)
            val fd = server.acceptFd()
            val stream = ReactorStream(fd, currentReactor())
            val reader = launch {
                try { stream.read(Buffer()) } catch (e: CancellationException) { cancelled.value = 1; throw e } finally { stream.close(); client.close() }
            }
            server.close()
            yield()
            // Both arrive from another thread after this block has returned (the root is draining).
            helper.executeAfter(100_000L) { result.complete(7); reader.cancel() }
        }
        helper.requestTermination().result
        assertEquals(7, got.value); assertEquals(1, resolved.value); assertEquals(1, cancelled.value)
    }

    /** (b) The loop reaches CLOSING only once no kernel op is in flight. */
    @Test
    fun closingWaitsForKernelOps() {
        val reactorRef = AtomicReference<Reactor?>(null)
        runReactor {
            reactorRef.value = currentReactor()
            val server = listenTcpServer("127.0.0.1", 21991, SocketOptions.Default)
            val client = connect("127.0.0.1", 21991)
            val stream = ReactorStream(server.acceptFd(), currentReactor())
            val reader = launch { runCatching { stream.read(Buffer()) } }
            yield()
            stream.close()                          // the read (an io_uring RECV) may still be in the kernel
            reader.join(); client.close(); server.close()
        }
        val r = reactorRef.value!!
        assertEquals(0, r.inFlightAtClosing, "kernel ops in flight when CLOSING was entered")
        assertEquals(Reactor.STOPPED, r.lifecycleState)
    }

    /** (c) Posts racing the close of the entry: each is either run exactly once or refused. */
    @Test
    fun externalPostsRacingCloseAreRunOrRefused() {
        val poster = Worker.start(name = "lifecycle-c")
        var total = 0
        repeat(300) {
            val ref = AtomicReference<Reactor?>(null)
            val accepted = AtomicInt(0); val ran = AtomicInt(0); val attempts = AtomicInt(0)
            val done = poster.execute(TransferMode.SAFE, { Triple(ref, accepted, Pair(ran, attempts)) }) { (r, acc, p) ->
                val (runCount, tries) = p
                while (true) {
                    val reactor = r.value ?: continue
                    tries.incrementAndGet()
                    if (reactor.tryDispatchExternal(kotlinx.coroutines.Runnable { runCount.incrementAndGet() })) acc.incrementAndGet() else break
                }
            }
            runReactor { ref.value = currentReactor(); delay(1) }
            done.result
            assertEquals(accepted.value, ran.value, "an accepted post was lost or run twice")
            total += attempts.value
        }
        poster.requestTermination().result
        println("ReactorLifecycleTest.externalPostsRacingCloseAreRunOrRefused: $total posts")
        assertTrue(total >= 300)
    }

    /** (d) On its own thread a resume is queued: code after resume() runs before the resumed coroutine. */
    @Test
    fun resumeOnOwnThreadIsQueued() = runReactor {
        val resumer = reactorResumer(coroutineContext)!!
        val slot = AtomicReference<Continuation<Int>?>(null)
        val afterCall = AtomicInt(0)
        val parked = async {
            val v = suspendCoroutineUninterceptedOrReturn<Int> { c -> slot.value = c; COROUTINE_SUSPENDED }
            v * 10 + afterCall.value
        }
        while (slot.value == null) yield()
        assertTrue(resumer.resume(slot.value!!, 4))
        afterCall.value = 1
        assertEquals(41, parked.await())
    }

    /** (e) With the slot convention, a cancel from another thread racing a resume resumes exactly once. */
    @Test
    fun slotConventionResumesExactlyOnce() {
        val canceller = Worker.start(name = "lifecycle-e")
        val n = 10_000
        val resumes = AtomicInt(0)
        runReactor {
            val reactor = currentReactor()
            val resumer = reactorResumer(coroutineContext)!!
            repeat(n) {
                val slot = AtomicReference<Continuation<Unit>?>(null)
                val job = launch {
                    suspendCoroutineUninterceptedOrReturn<Unit> { c -> slot.value = c; COROUTINE_SUSPENDED }
                }
                while (slot.value == null) yield()
                // The cancellation path posts to the reactor before touching the slot.
                job.invokeOnCompletion(onCancelling = true) { cause ->
                    if (cause != null) reactor.postToReactor {
                        slot.value?.let { c -> slot.value = null; resumer.resumeWithException(c, CancellationException("cancelled")); resumes.incrementAndGet() }
                    }
                }
                canceller.execute(TransferMode.SAFE, { job }) { it.cancel() }
                // The normal path, racing it.
                slot.value?.let { c -> slot.value = null; resumer.resume(c, Unit); resumes.incrementAndGet() }
                job.join()
                assertEquals(it + 1, resumes.value, "iteration $it resumed ${resumes.value - it} times")
            }
        }
        canceller.requestTermination().result
        assertEquals(n, resumes.value)
    }

    /**
     * (f) A root block that fails cancels the coroutines it started (like runBlocking), and runReactor
     * rethrows the failure. Before, the failure was swallowed while a parked child (an accept loop, a
     * read) kept the root waiting forever, so a failing test hung instead of failing.
     */
    @Test
    fun failingRootCancelsParkedChildrenAndRethrows() {
        val cancelledChildren = AtomicInt(0)
        val e = kotlin.test.assertFailsWith<IllegalStateException> {
            runReactor {
                val l = listenTcpServer("127.0.0.1", 21992, SocketOptions.Default)
                val c = connect("127.0.0.1", 21992)
                val s = ReactorStream(l.acceptFd(), currentReactor())
                launch { try { while (true) l.acceptFd() } catch (x: CancellationException) { cancelledChildren.incrementAndGet(); throw x } }
                launch { try { s.read(Buffer()) } catch (x: CancellationException) { cancelledChildren.incrementAndGet(); throw x } finally { s.close(); c.close() } }
                delay(20)
                error("boom")
            }
        }
        assertEquals("boom", e.message)
        assertEquals(2, cancelledChildren.value)
        // A child that fails fails the whole scope: runReactor rethrows the child's own exception
        // (it used to reach the uncaught-exception handler and abort the process).
        val childFailure = kotlin.test.assertFailsWith<IllegalArgumentException> {
            runReactor {
                val l = listenTcpServer("127.0.0.1", 21994, SocketOptions.Default)
                launch { while (true) l.acceptFd() }
                launch { delay(10); throw IllegalArgumentException("child") }
                delay(10_000)
            }
        }
        assertEquals("child", childFailure.message)
        // A timeout escaping the root behaves the same way.
        kotlin.test.assertFailsWith<kotlinx.coroutines.TimeoutCancellationException> {
            runReactor {
                val l = listenTcpServer("127.0.0.1", 21993, SocketOptions.Default)
                launch { while (true) l.acceptFd() }
                kotlinx.coroutines.withTimeout(50) { delay(1_000) }
            }
        }
    }
}
