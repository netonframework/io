package neton.io.net

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Delay
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.launch
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.time.TimeSource
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import neton.io.bytes.Buffer
import platform.posix.fflush
import platform.posix.fprintf
import platform.posix.getenv
import platform.posix.stderr
import kotlin.coroutines.CoroutineContext

/**
 * Per-reactor event-loop counters, for explaining benchmark results with counts instead of
 * sampling guesses. Enabled by NETON_IO_STATS=1; when disabled the counters are never touched
 * (one branch per event), and the cost of enabling them is itself measurable with an A/B run.
 * Printed once, as one JSON line on stderr prefixed `NETON_IO_STATS`, when the reactor stops.
 */
internal class ReactorStats {
    var rounds = 0L            // loop iterations
    var tasksRun = 0L          // dispatched continuations executed
    var maxTasksInRound = 0L
    var polls = 0L             // poll()/io_uring_enter calls that could wait
    var pollsZeroTimeout = 0L  // polls made with timeout 0 because tasks were still pending
    var pollsNoEvents = 0L     // polls that returned no event
    var events = 0L            // ready fds / CQEs delivered
    var maxEventsInPoll = 0L
    var reads = 0L; var readBytes = 0L; var readsWouldBlock = 0L
    var writes = 0L; var writeBytes = 0L; var writesWouldBlock = 0L

    fun json(driver: String, taskBudget: Int): String =
        "{\"driver\":\"$driver\",\"task_budget\":$taskBudget,\"rounds\":$rounds,\"tasks_run\":$tasksRun," +
        "\"max_tasks_in_round\":$maxTasksInRound,\"polls\":$polls,\"polls_zero_timeout\":$pollsZeroTimeout," +
        "\"polls_no_events\":$pollsNoEvents,\"events\":$events,\"max_events_in_poll\":$maxEventsInPoll," +
        "\"reads\":$reads,\"read_bytes\":$readBytes,\"reads_would_block\":$readsWouldBlock," +
        "\"writes\":$writes,\"write_bytes\":$writeBytes,\"writes_would_block\":$writesWouldBlock}"
}

/**
 * The reactor: a single-threaded scheduler that is also the [CoroutineDispatcher] for the
 * coroutines it drives, and that exposes the connection I/O operations as suspend functions.
 *
 * Two families implement it:
 * - readiness (kqueue/epoll/poll): a suspend read waits for the fd to be ready, then does recv;
 * - completion (io_uring/IOCP): a suspend read submits the op with the buffer and awaits the
 *   completion — the buffer is part of the model.
 *
 * The upper layers ([ReactorStream], TCP helpers) are written against these operations and do
 * not know which family is underneath.
 */
/**
 * Threading contract. The reactor is single-threaded: connection state, buffers and waiters are
 * touched only on the thread running [runUntil]. The one supported cross-thread entry is
 * [dispatch]: a coroutine resumed from another thread (a `CompletableDeferred` completed by a
 * worker, a callback from a foreign library) is queued on a lock-free external queue and the
 * loop is woken through a self-pipe. I/O calls (read/write/close) from another thread are
 * rejected with IllegalStateException rather than silently corrupting state. Timers are part
 * of the loop ([Delay]), so `delay` / `withTimeout` never leave the reactor thread.
 */
@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class, InternalCoroutinesApi::class)
internal abstract class Reactor : CoroutineDispatcher(), Delay {

    private val tasks = ArrayDeque<Runnable>()

    // ---- cross-thread dispatch: MPSC stack + self-pipe wakeup
    private class ExtNode(val block: Runnable, val next: ExtNode?)
    private val external = AtomicReference<ExtNode?>(null)
    private val wakePipe: IntArray = createWakePipe()
    protected val wakeReadFd: Int get() = wakePipe[0]

    // ---- timers (min-heap by deadline, lazy cancellation)
    private class Timer(val deadlineNs: Long, val seq: Long, val block: Runnable) : DisposableHandle {
        var cancelled = false
        override fun dispose() { cancelled = true }
    }
    private val timers = ArrayList<Timer>()
    private var timerSeq = 0L
    private val clock = TimeSource.Monotonic.markNow()
    private fun nowNs(): Long = clock.elapsedNow().inWholeNanoseconds

    /** Null unless NETON_IO_STATS=1. Subclasses count only when non-null. */
    protected val stats: ReactorStats? = if (getenv("NETON_IO_STATS")?.toKString() == "1") ReactorStats() else null

    /**
     * Max dispatched tasks to run per loop round before giving the poller a turn (0 = unbounded,
     * the original behaviour: drain everything, then block in the poller). NETON_IO_TASK_BUDGET.
     * With a budget, tasks still pending after the round make the next poll non-blocking, so I/O
     * readiness is checked every `budget` tasks instead of only when the task queue is empty. Note:
     * the reactor has no timers yet; when it does, a budget must not delay them either.
     */
    protected val taskBudget: Int = getenv("NETON_IO_TASK_BUDGET")?.toKString()?.toIntOrNull() ?: 0

    protected abstract val driverName: String

    /** The thread that runs the loop; set by [runUntil] callers via [bindOwner]. */
    private var ownerThread: ULong = 0uL

    fun bindOwner() { ownerThread = currentThreadId() }

    /** True when called on the reactor's own thread. */
    fun isOwnerThread(): Boolean = ownerThread == 0uL || ownerThread == currentThreadId()

    protected fun checkOwner(what: String) {
        check(isOwnerThread()) { "$what must be called on the reactor thread that owns the stream" }
    }

    fun checkOwnerPublic(what: String) = checkOwner(what)

    protected fun closeWakePipe() { closeFd(wakePipe[0]); closeFd(wakePipe[1]) }

    /**
     * Close a stream's fd: fail every coroutine parked on it with [neton.io.core.ClosedException],
     * drop the driver's interest, then close. Reactor thread only.
     */
    abstract fun closeStream(fd: Int)

    final override fun dispatch(context: CoroutineContext, block: Runnable) {
        if (isOwnerThread()) {
            tasks.addLast(block)
            return
        }
        // Another thread: push (lock-free) and wake the loop.
        while (true) {
            val head = external.load()
            if (external.compareAndSet(head, ExtNode(block, head))) break
        }
        signalWakePipe(wakePipe[1])
    }

    /** Move externally posted tasks (if any) onto the local queue, oldest first. */
    protected fun absorbExternal() {
        var node: ExtNode? = external.exchange(null) ?: return
        val batch = ArrayList<Runnable>()
        while (node != null) { batch.add(node.block); node = node.next }
        for (i in batch.indices.reversed()) tasks.addLast(batch[i])
    }

    /** Called when the wake pipe is readable: drain it and absorb the external queue. */
    protected fun onWake() {
        drainWakePipe(wakePipe[0])
        absorbExternal()
    }

    // ---- Delay: timers run on the reactor thread
    private fun addTimer(delayMs: Long, block: Runnable): Timer {
        val t = Timer(nowNs() + delayMs.coerceAtLeast(0) * 1_000_000L, timerSeq++, block)
        timers.add(t); siftUp(timers.size - 1)
        return t
    }

    override fun scheduleResumeAfterDelay(timeMillis: Long, continuation: CancellableContinuation<Unit>) {
        val t = addTimer(timeMillis) { with(continuation) { resumeUndispatched(Unit) } }
        continuation.invokeOnCancellation { t.dispose() }
    }

    override fun invokeOnTimeout(timeMillis: Long, block: Runnable, context: CoroutineContext): DisposableHandle =
        addTimer(timeMillis, block)

    /** Milliseconds until the next live timer (0 if due), or -1 when there is none. */
    protected fun nextTimerMillis(): Int {
        while (timers.isNotEmpty() && timers[0].cancelled) popTimer()
        if (timers.isEmpty()) return -1
        val ns = timers[0].deadlineNs - nowNs()
        return if (ns <= 0) 0 else ((ns + 999_999L) / 1_000_000L).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    }

    /** Run every timer whose deadline has passed. */
    protected fun fireTimers() {
        if (timers.isEmpty()) return
        val now = nowNs()
        while (timers.isNotEmpty() && (timers[0].cancelled || timers[0].deadlineNs <= now)) {
            val t = popTimer()
            if (!t.cancelled) t.block.run()
        }
    }

    private fun less(a: Timer, b: Timer) = a.deadlineNs < b.deadlineNs || (a.deadlineNs == b.deadlineNs && a.seq < b.seq)
    private fun siftUp(i0: Int) { var i = i0; while (i > 0) { val p = (i - 1) / 2; if (less(timers[i], timers[p])) { val x = timers[i]; timers[i] = timers[p]; timers[p] = x; i = p } else break } }
    private fun popTimer(): Timer {
        val top = timers[0]; val last = timers.removeAt(timers.size - 1)
        if (timers.isNotEmpty()) { timers[0] = last; var i = 0
            while (true) { val l = 2 * i + 1; val r = l + 1; var m = i
                if (l < timers.size && less(timers[l], timers[m])) m = l
                if (r < timers.size && less(timers[r], timers[m])) m = r
                if (m == i) break; val x = timers[i]; timers[i] = timers[m]; timers[m] = x; i = m } }
        return top
    }

    protected fun drainTasks() {
        var n = 0L
        while (tasks.isNotEmpty()) {
            tasks.removeFirst().run()
            n++
            if (taskBudget > 0 && n >= taskBudget) break
        }
        stats?.let { st ->
            st.rounds++
            st.tasksRun += n
            if (n > st.maxTasksInRound) st.maxTasksInRound = n
        }
    }

    /** Record one poll/enter: [timeoutZero] if it could not block, [events] delivered. */
    protected fun countPoll(timeoutZero: Boolean, events: Int) {
        val st = stats ?: return
        st.polls++
        if (timeoutZero) st.pollsZeroTimeout++
        if (events <= 0) st.pollsNoEvents++ else {
            st.events += events
            if (events > st.maxEventsInPoll) st.maxEventsInPoll = events.toLong()
        }
    }

    fun printStats() {
        val st = stats ?: return
        fprintf(stderr, "NETON_IO_STATS %s\n", st.json(driverName, taskBudget))
        fflush(stderr)
    }

    protected fun hasTasks(): Boolean = tasks.isNotEmpty()

    /** Read available bytes into [dst]; returns the count (>0) or -1 at EOF. Reactor thread only. */
    abstract suspend fun read(fd: Int, dst: Buffer, chunk: Int): Int

    /** Write all readable bytes from [src]; returns the number written. */
    abstract suspend fun write(fd: Int, src: Buffer): Int

    /** Accept one connection, returning a non-blocking client fd. */
    abstract suspend fun accept(listenFd: Int): Int

    /** Wait for a non-blocking connect on [fd] to complete. */
    abstract suspend fun awaitConnect(fd: Int)

    /** Drive the loop until [root] completes. */
    abstract fun runUntil(root: Job)

    abstract fun shutdown()

    companion object {
        fun run(block: suspend CoroutineScope.() -> Unit) {
            val reactor = createReactor()
            var failure: Throwable? = null
            val scope = CoroutineScope(reactor)
            val job = scope.launch(start = CoroutineStart.DEFAULT) {
                try {
                    block()
                } catch (t: Throwable) {
                    failure = t
                }
            }
            statsReactor = reactor
            reactor.bindOwner()
            reactor.runUntil(job)
            reactor.shutdown()
            reactor.printStats()
            statsReactor = null
            failure?.let { throw it }
        }
    }
}

/** Create the reactor for this platform, honoring NETON_IO_DRIVER. */
internal expect fun createReactor(): Reactor

/** [neton.io.core.IoStream] over a fd, delegating every operation to the [Reactor]. */
internal class ReactorStream(
    private val fd: Int,
    private val reactor: Reactor,
    private val readChunk: Int = 64 * 1024,
) : neton.io.core.IoStream {
    private var closed = false

    override suspend fun read(dst: Buffer): Int {
        if (closed) throw neton.io.core.ClosedException()
        reactor.checkOwnerPublic("read")
        return reactor.read(fd, dst, readChunk)
    }

    override suspend fun write(src: Buffer): Int {
        if (closed) throw neton.io.core.ClosedException()
        reactor.checkOwnerPublic("write")
        return reactor.write(fd, src)
    }

    override suspend fun flush() {}

    override fun close() {
        if (closed) return
        closed = true
        reactor.closeStream(fd)
    }
}

/** Identity of the calling OS thread (pthread_self), for ownership checks. */
internal expect fun currentThreadId(): ULong

/** The reactor currently driven by [Reactor.run] (last one started), for [dumpReactorStats]. */
private var statsReactor: Reactor? = null

/**
 * Print the running reactor's NETON_IO_STATS line now (no-op unless NETON_IO_STATS=1). Meant for
 * processes that are terminated externally (a benchmark server killed by the runner) and would
 * otherwise never reach the normal end-of-run print; call it from the termination path.
 */
fun dumpReactorStats() {
    statsReactor?.printStats()
}
