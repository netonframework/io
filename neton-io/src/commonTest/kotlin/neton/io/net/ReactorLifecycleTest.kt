@file:OptIn(kotlin.concurrent.atomics.ExperimentalAtomicApi::class, kotlinx.coroutines.InternalCoroutinesApi::class)

package neton.io.net

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §28.3: reactor lifecycle (DRAINING → CLOSING → STOPPED) and ReactorResumer accept / refuse. */
class ReactorLifecycleTest {

    /** (a) While draining, results from other threads, name lookups and cancellations still arrive. */
    @Test
    fun drainingDeliversWhatChildrenWaitFor() {
        val got = AtomicInt(-1); val resolved = AtomicInt(0); val cancelled = AtomicInt(0)
        val helper = startTestWorker("lifecycle-a")
        runReactor {
            val result = CompletableDeferred<Int>()
            launch { got.store(result.await()) }
            launch { if (resolve("localhost", 80, passive = false).isNotEmpty()) resolved.store(1) }
            val server = listenTcpServer("127.0.0.1", 21990, SocketOptions.Default)
            val client = connect("127.0.0.1", 21990)
            val fd = server.acceptFd()
            val stream = ReactorStream(fd, currentReactor())
            val reader = launch {
                try { stream.read(Buffer()) } catch (e: CancellationException) { cancelled.store(1); throw e } finally { stream.close(); client.close() }
            }
            server.close()
            yield()
            // Both arrive from another thread after this block has returned (the root is draining).
            helper.submitAfter(100_000L) { result.complete(7); reader.cancel() }
        }
        helper.stop()
        assertEquals(7, got.load()); assertEquals(1, resolved.load()); assertEquals(1, cancelled.load())
    }

    /** (b) The loop reaches CLOSING only once no kernel op is in flight. */
    @Test
    fun closingWaitsForKernelOps() {
        val reactorRef = AtomicReference<Reactor?>(null)
        runReactor {
            reactorRef.store(currentReactor())
            val server = listenTcpServer("127.0.0.1", 21991, SocketOptions.Default)
            val client = connect("127.0.0.1", 21991)
            val stream = ReactorStream(server.acceptFd(), currentReactor())
            val reader = launch { runCatching { stream.read(Buffer()) } }
            yield()
            stream.close()                          // the read (an io_uring RECV) may still be in the kernel
            reader.join(); client.close(); server.close()
        }
        val r = reactorRef.load()!!
        assertEquals(0, r.inFlightAtClosing, "kernel ops in flight when CLOSING was entered")
        assertEquals(Reactor.STOPPED, r.lifecycleState)
    }

    /** (c) Posts racing the close of the entry: each is either run exactly once or refused. */
    @Test
    fun externalPostsRacingCloseAreRunOrRefused() {
        val poster = startTestWorker("lifecycle-c")
        var total = 0
        repeat(300) {
            val ref = AtomicReference<Reactor?>(null)
            val accepted = AtomicInt(0); val ran = AtomicInt(0); val attempts = AtomicInt(0)
            val done = poster.submit {
                val r = ref; val acc = accepted; val runCount = ran; val tries = attempts
                while (true) {
                    val reactor = r.load() ?: continue
                    // Bounded backlog: a starved reactor thread (a loaded machine) must not let an unthrottled
                    // poster queue posts until the heap runs out. Accepted posts are always run, so the
                    // backlog drains while the reactor runs and when it closes; the race with close is intact.
                    if (acc.load() - runCount.load() > MAX_BACKLOG) continue
                    tries.addAndFetch(1)
                    if (reactor.tryDispatchExternal(kotlinx.coroutines.Runnable { runCount.addAndFetch(1) })) acc.addAndFetch(1) else break
                }
            }
            runReactor { ref.store(currentReactor()); delay(1) }
            done()
            assertEquals(accepted.load(), ran.load(), "an accepted post was lost or run twice")
            total += attempts.load()
        }
        poster.stop()
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
            val v = suspendCoroutineUninterceptedOrReturn<Int> { c -> slot.store(c); COROUTINE_SUSPENDED }
            v * 10 + afterCall.load()
        }
        while (slot.load() == null) yield()
        assertTrue(resumer.resume(slot.load()!!, 4))
        afterCall.store(1)
        assertEquals(41, parked.await())
    }

    /** (e) With the slot convention, a cancel from another thread racing a resume resumes exactly once. */
    @Test
    fun slotConventionResumesExactlyOnce() {
        val canceller = startTestWorker("lifecycle-e")
        val n = 10_000
        val resumes = AtomicInt(0)
        runReactor {
            val reactor = currentReactor()
            val resumer = reactorResumer(coroutineContext)!!
            repeat(n) {
                val slot = AtomicReference<Continuation<Unit>?>(null)
                val job = launch {
                    suspendCoroutineUninterceptedOrReturn<Unit> { c -> slot.store(c); COROUTINE_SUSPENDED }
                }
                while (slot.load() == null) yield()
                // The cancellation path posts to the reactor before touching the slot.
                job.invokeOnCompletion(onCancelling = true) { cause ->
                    if (cause != null) reactor.postToReactor {
                        slot.load()?.let { c -> slot.store(null); resumer.resumeWithException(c, CancellationException("cancelled")); resumes.addAndFetch(1) }
                    }
                }
                canceller.submit { job.cancel() }
                // The normal path, racing it.
                slot.load()?.let { c -> slot.store(null); resumer.resume(c, Unit); resumes.addAndFetch(1) }
                job.join()
                assertEquals(it + 1, resumes.load(), "iteration $it resumed ${resumes.load() - it} times")
            }
        }
        canceller.stop()
        assertEquals(n, resumes.load())
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
                launch { try { while (true) l.acceptFd() } catch (x: CancellationException) { cancelledChildren.addAndFetch(1); throw x } }
                launch { try { s.read(Buffer()) } catch (x: CancellationException) { cancelledChildren.addAndFetch(1); throw x } finally { s.close(); c.close() } }
                delay(20)
                error("boom")
            }
        }
        assertEquals("boom", e.message)
        assertEquals(2, cancelledChildren.load())
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

    private companion object {
        /** Posts accepted but not yet run that the poster in (c) lets accumulate before waiting. */
        const val MAX_BACKLOG = 10_000
    }
}
