@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
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

/** Tests (SPEC §27.8): the worker reactor with this index fails to start; -1 = none. */
@OptIn(ExperimentalAtomicApi::class)
internal val failWorkerStartForTest = AtomicInt(-1)

/**
 * Default reactor count, at least 1. Linux/Android count CPUs allowed by the calling thread's
 * affinity (including container cpusets); other platforms use their platform CPU count.
 * Not a CPU-bandwidth quota estimate. Set `reactors` explicitly for quota-only containers.
 * Call before pinning the calling thread if the group should use the original CPU set.
 */
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
    /** Server groups using these reactors (SPEC §27.5); the last to release stops them. Reactor 0 only. */
    private var users = 1
    /** Connections handled per reactor (diagnostics / tests). */
    val handled = Array(count) { AtomicInt(0) }

    fun member(i: Int): Member = members[i]!!

    /**
     * Start reactors 1..n-1 on worker threads and wait until each has reported its reactor and
     * scope. Reactor 0 is the caller's (passed in), which must already be running.
     */
    fun start(primary: Reactor, primaryScope: CoroutineScope, pinThreads: Boolean = false) {
        members[0] = Member(primary, primaryScope)
        val readies = ArrayList<CompletableDeferred<Member>>()
        for (i in 1 until count) {
            val ready = CompletableDeferred<Member>()
            val done = CompletableDeferred<Unit>()
            readies.add(ready); exited.add(done)
            val w = Worker.start(name = "neton-reactor-$i")
            workers.add(w)
            val stopRef = stop
            val pinTo = if (pinThreads) i else -1
            w.execute(TransferMode.SAFE, { Pair(Triple(ready, stopRef, done), Pair(pinTo, i)) }) { (t, p) ->
                val (r, s, d) = t
                val (pin, index) = p
                try {
                    if (pin >= 0) pinCurrentThread(pin)
                    if (failWorkerStartForTest.load() == index) error("test: reactor $index failed to start")
                    Reactor.runServing(s) { reactor, scope -> r.complete(Member(reactor, scope)) }
                } catch (e: Throwable) {
                    // Before `ready` was published (driver setup failed, ...): the starter must learn
                    // of it instead of waiting forever (SPEC §27.8). After it, nothing waits on `ready`.
                    r.completeExceptionally(e)
                } finally {
                    d.complete(Unit)
                }
            }
        }
        // Workers complete `ready` from their own threads; wait here (blocking on the acceptor
        // thread only during startup, before any connection exists). Every worker reports, success
        // or failure; if one failed, the others are told to exit and the failure is thrown.
        var failure: Throwable? = null
        for ((i, ready) in readies.withIndex()) {
            while (!ready.isCompleted) platform.posix.usleep(200u)
            @Suppress("OPT_IN_USAGE")
            val e = ready.getCompletionExceptionOrNull()
            @Suppress("OPT_IN_USAGE")
            if (e == null) members[i + 1] = ready.getCompleted() else if (failure == null) failure = e
        }
        if (failure != null) { stop(); throw failure }
    }

    /** Run [block] on reactor [i]'s thread: inline for reactor 0 (callers are on it), dispatched otherwise. */
    fun runOn(i: Int, block: () -> Unit) {
        if (i == 0) block() else member(i).reactor.dispatch(EmptyCoroutineContext, Runnable { block() })
    }

    /**
     * Like [runOn], but false instead of an exception when reactor [i] has already stopped (SPEC §28.3).
     * A reactor only stops after every coroutine in its scope has ended, so a refusal means there is
     * nothing of this group left on it.
     */
    fun tryRunOn(i: Int, block: () -> Unit): Boolean {
        if (i == 0) { block(); return true }
        return member(i).reactor.tryDispatchExternal(Runnable { block() })
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
        // Only a nudge to wake the loop; a worker that is already closing refuses it and needs none (SPEC §28.3).
        for (i in 1 until count) members[i]?.reactor?.tryDispatchExternal(Runnable { })
        // Queued behind the running reactor job: each thread ends when its reactor has.
        for (w in workers) w.requestTermination(processScheduledJobs = true)
    }

    /** Another server group shares these reactors (SPEC §27.5). */
    fun retain() { check(!stopped) { "the reactors have stopped" }; users++ }

    /** A server group is done with these reactors; the last one stops them. */
    fun release() { if (--users <= 0) stop() }

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
    // SPEC §27.10: set by a forced stop before it cancels the connections it can see; a connection
    // still queued then (handoff task or coroutine not yet run) checks it before its handler.
    private val forceStopped = AtomicInt(0)
    private var next = 0
    private var released = false

    /** Give the reactors back once (serve's end and shutdown both come here). Reactor 0 only. */
    private fun releaseReactors() { if (!released) { released = true; group.release() } }

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
            releaseReactors()
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
                // From accept until the handoff this loop owns the fd and the slot: a cancellation or a
                // close while paused must give both back (SPEC §27.8). onFd takes them over.
                var handedOff = false
                try {
                    pauseGate.load()?.await()          // paused: hold the accepted connection until resume
                    if (closing.load() != 0) throw ClosedException("server closed")
                    onFd(fd)
                    handedOff = true
                } finally {
                    if (!handedOff) { closeFd(fd); releaseSlot() }
                }
            }
        } catch (_: ClosedException) {
        }
    }

    /** On reactor [i]'s thread: the stream and its coroutine are born there. */
    private fun launchConnection(i: Int, fd: Int, handler: suspend (IoStream) -> Unit) {
        val m = group.member(i)
        startConnection(m.scope, m.reactor, fd, connJobs[i], ::releaseSlot, { forceStopped.load() != 0 }, handler)
    }

    /** Tests: reactor [i]'s scope and reactor. */
    internal fun memberForTest(i: Int): Pair<CoroutineScope, Reactor> = group.member(i).let { it.scope to it.reactor }

    /** Tests: connections handed to reactor [i] so far. */
    internal fun handledForTest(i: Int): Int = group.handled[i].load()

    /** Tests: run [block] on reactor [i]'s thread without waiting. */
    internal fun runOnForTest(i: Int, block: () -> Unit) = group.runOn(i, block)

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
            cancelConnections()
            while (active.load() > 0) delay(10)
        }
        releaseReactors()
    }

    /**
     * Cancel every connection of this group, including ones not yet running (a graceful [shutdown]
     * then ends at once). The flag is set first: on each reactor a connection's body and this
     * cancel task run one after the other, so the body either is in the set when the task runs or
     * sees the flag (SPEC §27.10). Reactor 0 only.
     */
    internal fun cancelConnections() {
        forceStopped.store(1)
        // A worker reactor that already stopped (no connections left: it only stops after its
        // coroutines end) refuses the post; there is nothing to cancel there (SPEC §28.3).
        for (i in 0 until reactors) group.tryRunOn(i) { for (j in connJobs[i].toList()) j.cancel() }
    }

    /**
     * Suspend until the worker reactors have exited (their connections have all ended). With groups
     * sharing reactors ([listenAlso]) that is after every one of them has stopped.
     */
    suspend fun awaitWorkers() { group.awaitExit() }

    /**
     * Listen on another port with the same reactors (SPEC §27.5, like geario's `Server::bind` for
     * several services): the returned group has its own handler (via [serve]), cap, pause and
     * shutdown; the reactors stop when every group sharing them has. Call on reactor 0, the thread
     * that created the first group.
     */
    suspend fun listenAlso(
        host: String,
        port: Int,
        options: SocketOptions = SocketOptions.Default,
        maxConnections: Int = 0,
        acceptMode: AcceptMode = this.acceptMode,
    ): TcpServerGroup {
        require(maxConnections >= 0) { "maxConnections must be >= 0" }
        check(currentReactor() === group.member(0).reactor) { "listenAlso must be called on reactor 0" }
        val mode = if (acceptMode == AcceptMode.ReusePort && reusePortBalancesLoad && reactors > 1) AcceptMode.ReusePort else AcceptMode.Handoff
        val opts = if (mode == AcceptMode.ReusePort) options.withReusePort() else options
        val ls = arrayOfNulls<TcpServer>(reactors)
        ls[0] = listenTcpServer(host, port, opts)
        if (mode == AcceptMode.ReusePort) {
            try {
                for (i in 1 until reactors) ls[i] = group.callOn(i) { listenTcpServer(host, port, opts) }
            } catch (t: Throwable) {
                ls[0]?.close()
                for (i in 1 until reactors) ls[i]?.let { l -> group.runOn(i) { l.close() } }
                throw t
            }
        }
        try { group.retain() } catch (t: Throwable) { for (l in ls) l?.close(); throw t }
        return TcpServerGroup(ls, group, reactors, mode, maxConnections)
    }

    /** Tests: run [block] on reactor [i]'s thread and wait for it. */
    internal suspend fun callOnForTest(i: Int, block: () -> Unit) = group.callOn(i) { block() }
}

/**
 * Listen on [host]:[port] and spread connections over [reactors] reactors (SPEC §18.1, §23.4). Must
 * be called inside a running reactor: that reactor is reactor 0; `reactors - 1` more are started on
 * their own threads. Handlers on reactor 0 run as children of the caller's job.
 *
 * [pinThreads] pins reactor i to the i-th CPU the process may run on (SPEC §27.2; Linux, Android,
 * Windows; ignored on Apple). Off by default: it only helps when nothing else competes for those CPUs.
 */
suspend fun listenGroup(
    host: String,
    port: Int,
    reactors: Int = cpuCount(),
    options: SocketOptions = SocketOptions.Default,
    maxConnections: Int = 0,
    acceptMode: AcceptMode = AcceptMode.Handoff,
    pinThreads: Boolean = false,
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
    // SPEC §27.2: capture the allowed CPUs before any thread is pinned (new threads inherit masks).
    if (pinThreads) captureAffinity()
    // SPEC §27.8: a reactor that fails to start fails the whole group; the port is released.
    try { group.start(currentReactor(), localScope, pinThreads) } catch (t: Throwable) { listeners[0]?.close(); throw t }
    if (pinThreads) pinCurrentThread(0)
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
 * Wait for a termination signal, then stop this server (SPEC §27.1, §27.7, as geario does):
 * [Signal.Int] stops at once (open connections are cancelled); [Signal.Term] and [Signal.Quit] stop
 * gracefully, letting connections finish for up to [gracefulTimeoutMillis], and any second one of
 * these signals meanwhile cancels the rest at once. The signals are handled only while this runs;
 * their previous actions are restored when it returns (or is cancelled). Call on reactor 0.
 */
suspend fun TcpServerGroup.shutdownOnSignal(gracefulTimeoutMillis: Long = 30_000): Signal =
    stopOnSignal(gracefulTimeoutMillis) {}

internal suspend fun TcpServerGroup.stopOnSignal(gracefulTimeoutMillis: Long, onSignal: () -> Unit): Signal =
    // One subscription for the whole stop: the second signal is queued even if it arrives before
    // the force wait starts (SPEC §27.8).
    withSignals(Signal.Int, Signal.Term, Signal.Quit) { sub ->
        val s = sub.receive()
        onSignal()
        if (s == Signal.Int) {
            shutdown(0)
        } else coroutineScope {
            val force = launch { sub.receive(); cancelConnections() }
            try { shutdown(gracefulTimeoutMillis) } finally { force.cancel() }
        }
        s
    }

/**
 * Serve [host]:[port] on [reactors] reactors (default one per core). Blocks the calling thread
 * until the server stops: when [until] completes, or — with [shutdownOnSignals], for an
 * application's entry point — on a termination signal (see [shutdownOnSignal] and
 * [shutdownTimeoutMillis]). Signals are left alone by default: a library must not change how its
 * host process reacts to them (SPEC §27.7). Each accepted connection runs [handler] on the reactor
 * it is pinned to. Returns after the worker reactors exit. [maxConnections] > 0 caps open connections
 * (see [listenGroup]; SPEC §28.12: it is what bounds the waiters of an [neton.io.core.Admission]).
 */
fun serveTcp(
    host: String,
    port: Int,
    reactors: Int = cpuCount(),
    until: CompletableDeferred<Unit>? = null,
    acceptMode: AcceptMode = AcceptMode.Handoff,
    options: SocketOptions = SocketOptions.Default,
    shutdownOnSignals: Boolean = false,
    shutdownTimeoutMillis: Long = 30_000,
    pinThreads: Boolean = false,
    maxConnections: Int = 0,
    handler: suspend (IoStream) -> Unit,
) {
    require(reactors >= 1)
    runReactor {
        val group = listenGroup(host, port, reactors, options, maxConnections, acceptMode, pinThreads)
        val serveJob = launch { group.serve(handler) }
        // All of these coroutines run on this reactor, so the flags need no synchronisation.
        var signalled = false
        val signalJob = if (shutdownOnSignals) launch {
            group.stopOnSignal(shutdownTimeoutMillis) { signalled = true }
        } else null
        val untilJob = until?.let { u -> launch { u.await(); if (!signalled) group.close() } }
        serveJob.join()
        untilJob?.cancel()
        // A signal-triggered shutdown closed the listener first (ending serve) and is still letting
        // connections finish: wait for it. Otherwise nobody needs a signal any more.
        if (signalJob != null) { if (signalled) signalJob.join() else signalJob.cancel() }
        group.awaitWorkers()
    }
}

/**
 * Start the coroutine serving one accepted connection [fd] on [reactor]; call on that reactor's thread
 * (SPEC §27.9). The coroutine owns the fd and the slot from here: it is started ATOMIC, so its body
 * always runs — even when [scope] is already cancelled or the job is cancelled, from any thread,
 * before it starts — and everything it owns is registered and given back inside the body, on
 * [reactor], exactly once: [jobs] (the reactor's own set) gets the job and loses it again, the
 * stream is closed (idempotent, so a handler that closed it is fine), and [released] runs. A job
 * cancelled before it started, or whose server [forceStopped] meanwhile, skips [handler].
 */
internal fun startConnection(
    scope: CoroutineScope,
    reactor: Reactor,
    fd: Int,
    jobs: MutableSet<Job>,
    released: () -> Unit,
    forceStopped: () -> Boolean,
    handler: suspend (IoStream) -> Unit,
): Job = scope.launch(start = CoroutineStart.ATOMIC) {
    val self = coroutineContext[Job]!!
    jobs.add(self)
    val stream = ReactorStream(fd, reactor)
    try {
        // Cancelled before it started, or its server was force-stopped while it was queued (and so
        // not in the set the stop cancelled; SPEC §27.10): the handler never runs.
        if (forceStopped()) throw kotlinx.coroutines.CancellationException("server stopped")
        ensureActive()
        handler(stream)
    } catch (e: kotlinx.coroutines.CancellationException) {
        throw e
    } catch (t: Throwable) {
        // One connection's failure (a peer reset, a throwing handler) ends that connection only.
        // Escaping here cancelled the reactor's scope: every connection on it died, and with one
        // reactor the whole server (macOS `nc -z` resets the connection).
        reportConnectionFault(t)
    } finally {
        stream.close()
        jobs.remove(self)
        released()
    }
}

/** One line on stderr for a connection that ended with an error; the server keeps running. */
@OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)
internal fun reportConnectionFault(t: Throwable) {
    platform.posix.fprintf(platform.posix.stderr, "neton-io: connection closed after error: %s\n", t.message ?: t.toString())
    platform.posix.fflush(platform.posix.stderr)
}
