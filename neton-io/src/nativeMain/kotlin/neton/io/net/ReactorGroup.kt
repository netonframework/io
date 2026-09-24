@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.launch
import neton.io.core.IoStream
import platform.posix._SC_NPROCESSORS_ONLN
import platform.posix.sysconf
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker

/** Online CPU count (sysconf), at least 1. */
fun cpuCount(): Int = sysconf(_SC_NPROCESSORS_ONLN).toInt().coerceAtLeast(1)

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
    private var next = 0
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
            readies.add(ready)
            val w = Worker.start(name = "neton-reactor-$i")
            workers.add(w)
            val stopRef = stop
            w.execute(TransferMode.SAFE, { ready to stopRef }) { (r, s) ->
                Reactor.runServing(s) { reactor, scope -> r.complete(Member(reactor, scope)) }
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

    /** Stop the worker reactors: complete their root jobs, then join the threads. */
    fun close() {
        stop.complete(Unit)
        for (i in 1 until count) members[i]?.reactor?.dispatch(EmptyCoroutineContext, Runnable { })
        for (w in workers) w.requestTermination(processScheduledJobs = true).result
    }
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
        val group = ReactorGroup(reactors)
        group.start(currentReactor(), this)
        val server = listenTcpServer(host, port)
        val acceptJob = launch {
            try {
                while (true) {
                    val fd = server.acceptFd()
                    group.handoff(fd, handler)
                }
            } catch (_: neton.io.core.ClosedException) {
            } finally {
                group.close()
            }
        }
        if (until != null) {
            until.await()
            server.close()
            acceptJob.join()
        } else {
            acceptJob.join()
        }
    }
}
