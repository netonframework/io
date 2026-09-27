@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import neton.io.core.ClosedException
import neton.io.core.IoStream
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker

/** Online CPU count, at least 1. */
expect fun cpuCount(): Int

/** Whether SO_REUSEPORT spreads connections across the sockets bound to a port (Linux/Android only). */
internal expect val reusePortBalancesLoad: Boolean

/**
 * How a [TcpServerGroup] distributes accepted connections (SPEC §23.4).
 * - [Handoff]: reactor 0 accepts and hands each fd round-robin to a reactor (SPEC §16).
 * - [ReusePort]: every reactor has its own SO_REUSEPORT listener on the port and accepts for itself;
 *   the kernel spreads connections, nothing crosses threads. Linux/Android only; elsewhere it falls
 *   back to [Handoff].
 */
enum class AcceptMode { Handoff, ReusePort }

/**
 * One reactor per core (SPEC §16). Reactor 0 runs on the calling thread; reactors 1..n-1 each run
 * on their own [Worker] thread. Share-nothing: each reactor owns its connections for life.
 */
@OptIn(ExperimentalAtomicApi::class)
internal class ReactorGroup(private val count: Int) {
    internal class Member(val reactor: Reactor, val scope: CoroutineScope)

    private val members = arrayOfNulls<Member>(count)
    private val stop = CompletableDeferred<Unit>()
    private val workers = ArrayList<Worker>()
    /** Completed by each worker thread after its reactor has shut down (SPEC §18.1). */
    private val exited = ArrayList<CompletableDeferred<Unit>>()
    private var stopped = false
    /** Connections handled per reactor (diagnostics / tests). */
    val handled = Array(count) { AtomicInt(0) }

    fun member(i: Int): Member = members[i]!!

    /**
     * Start reactors 1..n-1 on worker threads and wait until each has reported its reactor and
     * scope. Reactor 0 is the caller's (passed in), which must already be running.
     */
    fun start(primary: Reactor, primaryScope: CoroutineScope) {
        members[0] = Member(primary, primaryScope)
        val readies = ArrayList<CompletableDeferred<Member>>()
        for (i in 1 until count) {
            val ready = CompletableDeferred<Member>()
            val done = CompletableDeferred<Unit>()
            readies.add(ready); exited.add(done)
            val w = Worker.start(name = "neton-reactor-$i")
            workers.add(w)
            val stopRef = stop
            w.execute(TransferMode.SAFE, { Triple(ready, stopRef, done) }) { (r, s, d) ->
                try {
                    Reactor.runServing(s) { reactor, scope -> r.complete(Member(reactor, scope)) }
                } finally {
                    d.complete(Unit)
                }
            }
        }
        // Workers complete `ready` from their own threads; wait here (blocking on the acceptor
        // thread only during startup, before any connection exists).
        for ((i, ready) in readies.withIndex()) {
            while (!ready.isCompleted) platform.posix.usleep(200u)
            @Suppress("OPT_IN_USAGE")
            members[i + 1] = ready.getCompleted()
        }
    }

    /** Run [block] on reactor [i]'s thread: inline for reactor 0 (callers are on it), dispatched otherwise. */
    fun runOn(i: Int, block: () -> Unit) {
        if (i == 0) block() else member(i).reactor.dispatch(EmptyCoroutineContext, Runnable { block() })
    }

    /** Run [block] in a coroutine on reactor [i] and await its result from the caller's reactor. */
    suspend fun <T> callOn(i: Int, block: suspend () -> T): T {
        if (i == 0) return block()
        val result = CompletableDeferred<T>()
        runOn(i) {
            member(i).scope.launch {
                try { result.complete(block()) } catch (t: Throwable) { result.completeExceptionally(t) }
            }
        }
        return result.await()
    }

    /**
     * Tell the worker reactors to exit once their coroutines are gone, without waiting for that
     * (SPEC §18.1). Waiting here would block reactor 0 — and every connection it owns — for as long
     * as any worker still has a live connection. Idempotent. [awaitExit] waits.
     */
    fun stop() {
        if (stopped) return
        stopped = true
        stop.complete(Unit)
        for (i in 1 until count) members[i]?.reactor?.dispatch(EmptyCoroutineContext, Runnable { })
        // Queued behind the running reactor job: each thread ends when its reactor has.
        for (w in workers) w.requestTermination(processScheduledJobs = true)
    }

    /** Suspend until every worker reactor has shut down. */
    suspend fun awaitExit() { for (d in exited) d.await() }
}

/**
 * A TCP listener whose connections are spread over [reactors] reactors (SPEC §18.1, §23.4). Created
 * by [listenGroup] inside a running reactor, which becomes reactor 0.
 *
 * Controls (reactor 0 only): [pause] / [resume] accepting, [close] (stop accepting), [shutdown]
 * (stop accepting, let connections finish up to a timeout, then cancel the rest). [maxConnections]
 * caps concurrent connections: at the cap, accepting waits until one ends.
 */
@OptIn(ExperimentalAtomicApi::class)
class TcpServerGroup internal constructor(
    private val listeners: Array<TcpServer?>,
    private val group: ReactorGroup,
    /** Number of reactors connections are spread over (including reactor 0). */
    val reactors: Int,
    /** The mode in effect ([AcceptMode.ReusePort] falls back to [AcceptMode.Handoff] where unsupported). */
    val acceptMode: AcceptMode,
    /** Concurrent connection cap; 0 = unlimited. */
    val maxConnections: Int,
) {
    // Slots: reserved before accept (so parallel accept loops cannot overshoot the cap), released
    // when the connection's coroutine ends — possibly on another reactor's thread.
    private val active = AtomicInt(0)
    private val slotFreed = AtomicReference<CompletableDeferred<Unit>?>(null)
    // Pause gate: non-null while paused; accept loops wait on it before handing a connection out.
    private val pauseGate = AtomicReference<CompletableDeferred<Unit>?>(null)
    // Connection coroutines per reactor, touched only on that reactor's thread (for shutdown).
    private val connJobs = Array(reactors) { HashSet<Job>() }
    // Set by close(); accept loops on any reactor check it after every wait.
    private val closing = AtomicInt(0)
    private var next = 0

    /** Connections currently being served (plus accept slots reserved by parked accept loops). */
    val activeConnections: Int get() = active.load()

    /**
     * Accept until [close]; each connection runs [handler] in a new coroutine on the reactor it is
     * pinned to. With [reactors] > 1 the handler runs on another thread than the caller, so any
     * shared state it touches must be thread-safe. Returns after [close]; the worker reactors are
     * then told to exit once their connections end (see [awaitWorkers]).
     */
    suspend fun serve(handler: suspend (IoStream) -> Unit) {
        try {
            if (acceptMode == AcceptMode.ReusePort) {
                for (i in 1 until reactors) group.runOn(i) {
                    group.member(i).scope.launch { acceptLoop(listeners[i]!!) { fd -> launchConnection(i, fd, handler) } }
                }
            }
            acceptLoop(listeners[0]!!) { fd ->
                if (acceptMode == AcceptMode.ReusePort) launchConnection(0, fd, handler)
                else {
                    val i = next; next = (next + 1) % reactors
                    group.handled[i].addAndFetch(1)
                    group.runOn(i) { launchConnection(i, fd, handler) }
                }
            }
        } finally {
            group.stop()
        }
    }

    private suspend fun acceptLoop(server: TcpServer, onFd: (Int) -> Unit) {
        try {
            while (true) {
                reserveSlot()
                val fd = try {
                    if (closing.load() != 0) throw ClosedException("server closed")
                    server.acceptFd()
                } catch (t: Throwable) { releaseSlot(); throw t }
                pauseGate.load()?.await()          // paused: hold the accepted connection until resume
                if (closing.load() != 0) { closeFd(fd); releaseSlot(); throw ClosedException("server closed") }
                onFd(fd)
            }
        } catch (_: ClosedException) {
        }
    }

    /** On reactor [i]'s thread: the stream and its coroutine are born there. */
    private fun launchConnection(i: Int, fd: Int, handler: suspend (IoStream) -> Unit) {
        val m = group.member(i)
        val job = m.scope.launch(start = CoroutineStart.LAZY) {
            val stream = ReactorStream(fd, m.reactor)
            try {
                handler(stream)
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (t: Throwable) {
                // One connection's failure (a peer reset, a throwing handler) ends that connection
                // only. Escaping here cancelled the reactor's scope: every connection on it died, and
                // with one reactor the whole server (macOS `nc -z` resets the connection).
                reportConnectionFault(t)
                stream.close()
            } finally {
                connJobs[i].remove(coroutineContext[Job])
                releaseSlot()
            }
        }
        connJobs[i].add(job)
        job.start()
    }

    private suspend fun reserveSlot() {
        if (maxConnections <= 0 || closing.load() != 0) { active.addAndFetch(1); return }
        while (true) {
            if (active.addAndFetch(1) <= maxConnections) return
            active.addAndFetch(-1)
            // Publish the waiter before re-checking, so a release in between cannot be missed.
            var gate = slotFreed.load()
            if (gate == null) { val d = CompletableDeferred<Unit>(); gate = if (slotFreed.compareAndSet(null, d)) d else slotFreed.load() }
            if (active.load() < maxConnections || closing.load() != 0) continue
            gate?.await()
        }
    }

    private fun releaseSlot() {
        active.addAndFetch(-1)
        slotFreed.exchange(null)?.complete(Unit)
    }

    /** Stop handing out newly accepted connections until [resume] (they wait, accepted but unhandled). */
    fun pause() { pauseGate.compareAndSet(null, CompletableDeferred()) }

    fun resume() { pauseGate.exchange(null)?.complete(Unit) }

    /** Stop accepting. Reactor 0 only (the one that called [listenGroup]). */
    fun close() {
        closing.store(1)
        resume()
        slotFreed.exchange(null)?.complete(Unit)   // wake accept loops waiting at the connection cap
        listeners[0]?.close()
        if (acceptMode == AcceptMode.ReusePort) for (i in 1 until reactors) {
            val l = listeners[i] ?: continue
            group.runOn(i) { l.close() }
        }
    }

    /**
     * Graceful stop (SPEC §23.4): stop accepting, wait up to [gracefulTimeoutMillis] for the served
     * connections to end on their own, then cancel the rest and wait for them to unwind.
     */
    suspend fun shutdown(gracefulTimeoutMillis: Long) {
        close()
        withTimeoutOrNull(gracefulTimeoutMillis) { while (active.load() > 0) delay(10) }
        if (active.load() > 0) {
            for (i in 0 until reactors) group.runOn(i) { for (j in connJobs[i].toList()) j.cancel() }
            while (active.load() > 0) delay(10)
        }
        group.stop()
    }

    /** Suspend until the worker reactors have exited (their connections have all ended). */
    suspend fun awaitWorkers() { group.awaitExit() }
}

/**
 * Listen on [host]:[port] and spread connections over [reactors] reactors (SPEC §18.1, §23.4). Must
 * be called inside a running reactor: that reactor is reactor 0; `reactors - 1` more are started on
 * their own threads. Handlers on reactor 0 run as children of the caller's job.
 */
suspend fun listenGroup(
    host: String,
    port: Int,
    reactors: Int = cpuCount(),
    options: SocketOptions = SocketOptions.Default,
    maxConnections: Int = 0,
    acceptMode: AcceptMode = AcceptMode.Handoff,
): TcpServerGroup {
    require(reactors >= 1) { "reactors must be >= 1" }
    require(maxConnections >= 0) { "maxConnections must be >= 0" }
    val mode = if (acceptMode == AcceptMode.ReusePort && reusePortBalancesLoad && reactors > 1) AcceptMode.ReusePort else AcceptMode.Handoff
    val opts = if (mode == AcceptMode.ReusePort) options.withReusePort() else options
    val localScope = CoroutineScope(coroutineContext)
    // Bind first: if the port is taken, no threads have been started.
    val listeners = arrayOfNulls<TcpServer>(reactors)
    listeners[0] = listenTcpServer(host, port, opts)
    val group = ReactorGroup(reactors)
    group.start(currentReactor(), localScope)
    if (mode == AcceptMode.ReusePort) {
        try {
            for (i in 1 until reactors) listeners[i] = group.callOn(i) { listenTcpServer(host, port, opts) }
        } catch (t: Throwable) {
            listeners[0]?.close()
            for (i in 1 until reactors) listeners[i]?.let { l -> group.runOn(i) { l.close() } }
            group.stop()
            throw t
        }
    }
    return TcpServerGroup(listeners, group, reactors, mode, maxConnections)
}

/**
 * Serve [host]:[port] on [reactors] reactors (default one per core). Blocks the calling thread
 * until [until] completes (never, by default — the process ends the server). Each accepted
 * connection runs [handler] on the reactor it is pinned to.
 */
fun serveTcp(
    host: String,
    port: Int,
    reactors: Int = cpuCount(),
    until: CompletableDeferred<Unit>? = null,
    acceptMode: AcceptMode = AcceptMode.Handoff,
    handler: suspend (IoStream) -> Unit,
) {
    require(reactors >= 1)
    runReactor {
        val group = listenGroup(host, port, reactors, acceptMode = acceptMode)
        val serveJob = launch { group.serve(handler) }
        if (until != null) {
            until.await()
            group.close()
            serveJob.join()
            group.awaitWorkers()
        } else {
            serveJob.join()
        }
    }
}

/** One line on stderr for a connection that ended with an error; the server keeps running. */
@OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)
internal fun reportConnectionFault(t: Throwable) {
    platform.posix.fprintf(platform.posix.stderr, "neton-io: connection closed after error: %s\n", t.message ?: t.toString())
    platform.posix.fflush(platform.posix.stderr)
}

