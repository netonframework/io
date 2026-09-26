@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.launch
import neton.io.core.ClosedException
import neton.io.core.IoStream
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker

/** Online CPU count, at least 1. */
expect fun cpuCount(): Int

/**
 * One reactor per core (SPEC §16). Reactor 0 runs on the calling thread and is the acceptor;
 * reactors 1..n-1 each run on their own [Worker] thread. Accepted connections are handed out
 * round-robin: the fd is delivered to the target reactor through its own `dispatch()` (MPSC
 * queue + self-pipe wakeup), and the [ReactorStream] plus the handler coroutine are created
 * *on the target thread*, so the connection is owned by that reactor for life and the acceptor
 * never registers interest in it. Share-nothing hot path; no cross-thread locks.
 */
@OptIn(ExperimentalAtomicApi::class)
internal class ReactorGroup(private val count: Int) {
    private class Member(val reactor: Reactor, val scope: CoroutineScope)

    private val members = arrayOfNulls<Member>(count)
    private val stop = CompletableDeferred<Unit>()
    private val workers = ArrayList<Worker>()
    /** Completed by each worker thread after its reactor has shut down (SPEC §18.1). */
    private val exited = ArrayList<CompletableDeferred<Unit>>()
    private var next = 0
    private var stopped = false
    /** Connections handled per reactor (diagnostics / tests). */
    val handled = Array(count) { AtomicInt(0) }

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

    /** Hand [fd] to the next reactor round-robin and run [handler] on it. */
    fun handoff(fd: Int, handler: suspend (IoStream) -> Unit) {
        val idx = next; next = (next + 1) % count
        val m = members[idx]!!
        handled[idx].addAndFetch(1)
        if (idx == 0) {
            m.scope.launch { handler(ReactorStream(fd, m.reactor)) }
        } else {
            // Runs on the target reactor thread; the stream and coroutine are born there.
            m.reactor.dispatch(EmptyCoroutineContext, Runnable {
                m.scope.launch { handler(ReactorStream(fd, m.reactor)) }
            })
        }
    }

    /**
     * Tell the worker reactors to exit once their connections are gone, without waiting for
     * that (SPEC §18.1). Waiting here would block reactor 0 — and every connection it owns — for
     * as long as any worker still has a live connection. Idempotent. [awaitExit] waits.
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
 * A TCP listener whose connections are spread over [reactors] reactors (SPEC §18.1). Created by
 * [listenGroup] inside a running reactor, which becomes reactor 0 and does the accepting.
 */
class TcpServerGroup internal constructor(
    private val server: TcpServer,
    private val group: ReactorGroup?,
    private val localScope: CoroutineScope,
    /** Number of reactors connections are spread over (including the accepting one). */
    val reactors: Int,
) {
    /**
     * Accept until [close]; each connection runs [handler] in a new coroutine on the reactor it
     * is pinned to. With [reactors] > 1 the handler runs on another thread than the caller, so
     * any shared state it touches must be thread-safe. Returns normally after [close]; the worker
     * reactors are told to exit once their connections end (see [awaitWorkers]).
     */
    suspend fun serve(handler: suspend (IoStream) -> Unit) {
        try {
            while (true) {
                if (group == null) {
                    val stream = server.accept()
                    localScope.launch { handler(stream) }
                } else {
                    group.handoff(server.acceptFd(), handler)
                }
            }
        } catch (_: ClosedException) {
        } finally {
            group?.stop()
        }
    }

    /** Stop accepting. Reactor 0 only (the one that called [listenGroup]). */
    fun close() = server.close()

    /** Suspend until the worker reactors have exited (their connections have all ended). */
    suspend fun awaitWorkers() { group?.awaitExit() }
}

/**
 * Listen on [host]:[port] and spread connections over [reactors] reactors (SPEC §18.1). Must be
 * called inside a running reactor: that reactor accepts and is reactor 0; `reactors - 1` more
 * are started on their own threads. Handlers on reactor 0 run as children of the caller's job.
 */
suspend fun listenGroup(host: String, port: Int, reactors: Int = cpuCount()): TcpServerGroup {
    require(reactors >= 1) { "reactors must be >= 1" }
    val localScope = CoroutineScope(coroutineContext)
    // Bind first: if the port is taken, no threads have been started.
    val server = listenTcpServer(host, port)
    val group = if (reactors > 1) ReactorGroup(reactors).also { it.start(currentReactor(), localScope) } else null
    return TcpServerGroup(server, group, localScope, reactors)
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
    handler: suspend (IoStream) -> Unit,
) {
    require(reactors >= 1)
    runReactor {
        val group = listenGroup(host, port, reactors)
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
