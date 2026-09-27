package neton.io.net

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
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
import kotlin.coroutines.EmptyCoroutineContext

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

    // ---- SPEC §19.3: resume queue for coroutines parked on I/O without a CancellableContinuation.
    // A ring of (continuation, error) pairs drained by [drainTasks] ahead of ordinary tasks: queued
    // (not resumed inline from the poll callback, which §17c step 4 measured slower), on this
    // thread, and without allocating a Runnable or a DispatchedTask per resume.
    private var resumeConts = arrayOfNulls<kotlin.coroutines.Continuation<Unit>>(256)
    private var resumeErrs = arrayOfNulls<Throwable>(256)
    private var resumeHead = 0
    private var resumeCount = 0

    // Same, for completion-driver ops whose result is an Int (byte count, fd) — a separate ring so the
    // value needs no box on the way through.
    private var intConts = arrayOfNulls<kotlin.coroutines.Continuation<Int>>(64)
    private var intVals = IntArray(64)
    private var intErrs = arrayOfNulls<Throwable>(64)
    private var intHead = 0
    // SPEC §24.11: same-thread handoffs between coroutines of one reactor (queues in protocol layers)
    // resume through this ring — an object value, no DispatchedTask, no context lookups.
    private var anyConts = arrayOfNulls<kotlin.coroutines.Continuation<Any?>>(64)
    private var anyVals = arrayOfNulls<Any?>(64)
    private var anyErrs = arrayOfNulls<Throwable>(64)
    private var anyHead = 0
    private var anyCount = 0

    /** Queue [cont] to be resumed with [value] (or [error]) on this reactor's next drain. Owner thread only. */
    internal fun enqueueResumeAny(cont: kotlin.coroutines.Continuation<*>, value: Any?, error: Throwable?) {
        if (anyCount == anyConts.size) {
            val n = anyConts.size
            val c2 = arrayOfNulls<kotlin.coroutines.Continuation<Any?>>(n * 2); val v2 = arrayOfNulls<Any?>(n * 2); val e2 = arrayOfNulls<Throwable>(n * 2)
            for (i in 0 until anyCount) { val j = (anyHead + i) % n; c2[i] = anyConts[j]; v2[i] = anyVals[j]; e2[i] = anyErrs[j] }
            anyConts = c2; anyVals = v2; anyErrs = e2; anyHead = 0
        }
        val i = (anyHead + anyCount) % anyConts.size
        @Suppress("UNCHECKED_CAST")
        anyConts[i] = cont as kotlin.coroutines.Continuation<Any?>; anyVals[i] = value; anyErrs[i] = error
        anyCount++
    }
    private var intCount = 0

    /** Queue [cont] to be resumed with [value] (or [error] if non-null) on the next drain. Owner thread only. */
    protected fun enqueueResumeInt(cont: kotlin.coroutines.Continuation<Int>, value: Int, error: Throwable? = null) {
        if (intCount == intConts.size) {
            val n = intConts.size
            val c2 = arrayOfNulls<kotlin.coroutines.Continuation<Int>>(n * 2); val v2 = IntArray(n * 2); val e2 = arrayOfNulls<Throwable>(n * 2)
            for (i in 0 until intCount) { val j = (intHead + i) % n; c2[i] = intConts[j]; v2[i] = intVals[j]; e2[i] = intErrs[j] }
            intConts = c2; intVals = v2; intErrs = e2; intHead = 0
        }
        val i = (intHead + intCount) % intConts.size
        intConts[i] = cont; intVals[i] = value; intErrs[i] = error
        intCount++
    }

    /** Queue [cont] to be resumed (with [error] if non-null) on this reactor's next drain. Owner thread only. */
    protected fun enqueueResume(cont: kotlin.coroutines.Continuation<Unit>, error: Throwable? = null) {
        if (resumeCount == resumeConts.size) {
            val n = resumeConts.size
            val c2 = arrayOfNulls<kotlin.coroutines.Continuation<Unit>>(n * 2)
            val e2 = arrayOfNulls<Throwable>(n * 2)
            for (i in 0 until resumeCount) { c2[i] = resumeConts[(resumeHead + i) % n]; e2[i] = resumeErrs[(resumeHead + i) % n] }
            resumeConts = c2; resumeErrs = e2; resumeHead = 0
        }
        val i = (resumeHead + resumeCount) % resumeConts.size
        resumeConts[i] = cont; resumeErrs[i] = error
        resumeCount++
    }

    // ---- cross-thread dispatch: MPSC stack + self-pipe wakeup
    private class ExtNode(val block: Runnable, val next: ExtNode?)
    private val external = AtomicReference<ExtNode?>(null)

    // ---- SPEC §28.3 lifecycle: RUNNING → DRAINING → CLOSING → STOPPED (only moves forward).
    // DRAINING: the root job is finishing; every post and resume is still accepted (the children's
    // endings depend on them). CLOSING: root done, no local work, no kernel op in flight; the external
    // entry is swapped for [CLOSED_ENTRY] with the same CAS the posters use, then everything accepted
    // before it runs. STOPPED: the loop has exited; every post is refused.
    private val lifecycle = kotlin.concurrent.atomics.AtomicInt(RUNNING)
    private var rootEndHandled = false

    /** Current lifecycle state (tests / diagnostics). */
    internal val lifecycleState: Int get() = lifecycle.load()

    /** Kernel operations still in flight (completion drivers override; readiness drivers have none). */
    protected open fun inFlightKernelOps(): Int = 0

    /** Once the root job is done: ask the kernel to cancel what is still in flight (completion drivers). */
    protected open fun cancelInFlightOps() {}

    /** Tests: kernel ops that were in flight when the loop entered CLOSING (must be 0). */
    internal var inFlightAtClosing: Int = -1
        private set

    /**
     * The driver loop calls this after draining a round; true means exit now (SPEC §28.3). Leaves the
     * loop running while the root job is still active, while local work is queued, or while the
     * kernel still owns buffers; then closes the external entry and runs what it had accepted.
     */
    protected fun readyToStop(root: Job): Boolean {
        if (!root.isCompleted) {
            if (!root.isActive) lifecycle.compareAndSet(RUNNING, DRAINING)
            return false
        }
        lifecycle.compareAndSet(RUNNING, DRAINING)
        if (!rootEndHandled) { rootEndHandled = true; cancelInFlightOps() }
        if (hasTasks() || inFlightKernelOps() > 0) return false
        inFlightAtClosing = inFlightKernelOps()
        lifecycle.store(CLOSING)
        var node = external.exchange(CLOSED_ENTRY)
        val batch = ArrayList<Runnable>()
        while (node != null) { batch.add(node.block); node = node.next }
        for (i in batch.indices.reversed()) tasks.addLast(batch[i])
        while (hasTasks()) drainTasks()
        lifecycle.store(STOPPED)
        return true
    }

    /**
     * Post [block] from another thread without throwing: false once the reactor no longer accepts
     * work (CLOSING / STOPPED, SPEC §28.3); true means it will run exactly once on this reactor.
     */
    internal fun tryDispatchExternal(block: Runnable): Boolean {
        while (true) {
            val head = external.load()
            if (head === CLOSED_ENTRY) return false
            if (external.compareAndSet(head, ExtNode(block, head))) break
        }
        wakeup()
        return true
    }

    /** On the owner thread: whether local work is still accepted (until the loop has exited). */
    internal fun acceptsLocalWork(): Boolean = lifecycle.load() != STOPPED
    // The self-pipe is created on first use (SPEC §23.1): readiness and io_uring drivers touch
    // [wakeReadFd] while setting up; IOCP overrides [wakeup] and never creates one.
    private val wakePipeLazy = lazy { createWakePipe() }
    private val wakePipe: IntArray by wakePipeLazy
    protected val wakeReadFd: Int get() = wakePipe[0]

    /** Wake the loop from another thread after [dispatch] queued work. Must be thread-safe. */
    protected open fun wakeup() { if (wakeClosed.load() == 0) signalWakePipe(wakePipe[1]) }

    // Set before the wake pipe is closed: a late cross-thread dispatch must not write to a closed
    // (or already reused) fd number, nor raise SIGPIPE on a pipe without a reader.
    private val wakeClosed = kotlin.concurrent.atomics.AtomicInt(0)

    // ---- timers (min-heap by deadline, lazy cancellation)
    private class Timer(val deadlineNs: Long, val seq: Long, val block: Runnable) : DisposableHandle {
        var cancelled = false
        override fun dispose() { cancelled = true }
    }
    private val timers = ArrayList<Timer>()
    private var timerSeq = 0L
    private val clock = TimeSource.Monotonic.markNow()
    private fun nowNs(): Long = clock.elapsedNow().inWholeNanoseconds
    protected fun reactorNowMs(): Long = nowNs() / 1_000_000L

    // ---- SPEC §23.2: connection timeouts. The clock is read once per loop round (in fireTimers);
    // streams stamp activity and deadlines with this cached value, so the hot path reads no clock.
    internal var cachedNowMs: Long = 0L
        private set

    /**
     * The clock for stamping stream deadlines and activity: the per-round cached value while the loop
     * keeps it fresh (something is timed), otherwise read now — the first deadline set on an idle
     * reactor must not be computed from a stale clock.
     */
    internal fun timeoutClockMs(): Long {
        if (wheel.size == 0 && timers.isEmpty()) {
            cachedNowMs = reactorNowMs()
            // The loop does not tick an empty wheel; bring its scan position to now before the first
            // deadline goes in, or the first scan could start past that deadline's slot.
            wheel.tick(cachedNowMs)
        }
        return cachedNowMs
    }
    internal val wheel = TimerWheel()

    /**
     * Wake the operations parked on [fd] — reads, writes or both — with [cause], as cancellation does
     * (completion drivers also cancel the op in the kernel where a late completion would be harmful).
     * Reactor thread only.
     */
    internal abstract fun timeoutParked(fd: Int, reads: Boolean, writes: Boolean, cause: Throwable)

    /** Null unless NETON_IO_STATS=1. Subclasses count only when non-null. */
    protected val stats: ReactorStats? = if (getenv("NETON_IO_STATS")?.toKString() == "1") ReactorStats() else null

    /**
     * Max dispatched tasks to run per loop round before the poller and the timers get a turn
     * (0 = unbounded: drain everything, then block). NETON_IO_TASK_BUDGET overrides the default.
     * The default is finite for fairness, not throughput: a coroutine that keeps re-dispatching
     * itself (a `yield()` loop, a busy connection whose every step resumes immediately) must not
     * starve I/O readiness and timers for the other connections (FairnessTest). Benchmarks showed
     * no throughput difference between 64 and unbounded at 1 in-flight (msgtrans bench results).
     * Tasks left over make the next poll non-blocking, so nothing is delayed beyond one round.
     */
    protected val taskBudget: Int = getenv("NETON_IO_TASK_BUDGET")?.toKString()?.toIntOrNull() ?: DEFAULT_TASK_BUDGET

    protected abstract val driverName: String

    /** The thread that runs the loop; set by [runUntil] callers via [bindOwner]. */
    private var ownerThread: ULong = 0uL

    /** This reactor thread's buffer pool, passed explicitly on the drivers' hot paths (SPEC §23.7). */
    internal var bufferPool: neton.io.bytes.BufferPool = neton.io.bytes.BufferPool.current
        private set

    fun bindOwner() { ownerThread = currentThreadId(); bufferPool = neton.io.bytes.BufferPool.current }

    /** True when called on the reactor's own thread. */
    fun isOwnerThread(): Boolean = ownerThread == 0uL || ownerThread == currentThreadId()

    protected fun checkOwner(what: String) {
        check(isOwnerThread()) { "$what must be called on the reactor thread that owns the stream" }
    }

    fun checkOwnerPublic(what: String) = checkOwner(what)

    protected fun closeWakePipe() {
        wakeClosed.store(1)
        if (wakePipeLazy.isInitialized()) { closeFd(wakePipe[1]); closeFd(wakePipe[0]) }
    }

    /**
     * Close a stream's fd: fail every coroutine parked on it with [neton.io.core.ClosedException],
     * drop the driver's interest, then close. Reactor thread only.
     */
    abstract fun closeStream(fd: Int)

    /** A stream over [fd] now exists on this reactor: register persistent interest if the driver has it (SPEC §17). */
    open fun registerStream(fd: Int) {}

    final override fun dispatch(context: CoroutineContext, block: Runnable) {
        if (isOwnerThread()) {
            if (lifecycle.load() == STOPPED) throw ReactorStoppedException("dispatch after the reactor stopped")
            tasks.addLast(block)
            return
        }
        // Another thread: push (lock-free) and wake the loop; refused once the entry is closed.
        if (!tryDispatchExternal(block)) throw ReactorStoppedException("dispatch to a reactor that is closing or stopped")
    }

    /**
     * Run [block] on the reactor thread. Cancellation handlers (`invokeOnCancellation`) may fire
     * on any thread, so waiter-map mutations must be funnelled back here instead of racing the
     * loop. On the owner thread this runs inline; otherwise it goes through the cross-thread
     * dispatch path (external queue + self-pipe wakeup).
     */
    fun postToReactor(block: () -> Unit) {
        if (isOwnerThread()) block()
        // Its callers are cancellation / cleanup hops. A reactor that is closing has no parked
        // operation of its scope left (SPEC §28.3), so a refused post has nothing to do.
        else tryDispatchExternal(Runnable { block() })
    }

    /** Move externally posted tasks (if any) onto the local queue, oldest first. */
    protected fun absorbExternal() {
        if (lifecycle.load() >= CLOSING) return          // the entry holds CLOSED_ENTRY now
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
        val wheelMs = if (wheel.size == 0) -1L else (wheel.nextDueMs - reactorNowMs()).coerceAtLeast(0L)
        if (timers.isEmpty()) return if (wheelMs < 0) -1 else wheelMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        val ns = timers[0].deadlineNs - nowNs()
        val heapMs = if (ns <= 0) 0L else ((ns + 999_999L) / 1_000_000L)
        val ms = if (wheelMs < 0) heapMs else minOf(heapMs, wheelMs)
        return ms.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    }

    protected fun fireTimers() {
        // The clock is read once per round, and only while something is timed (SPEC §23.2): with no
        // timers and no stream deadlines the loop reads no clock at all.
        if (wheel.size == 0 && timers.isEmpty()) return
        val now = nowNs()                                   // one clock read per round
        cachedNowMs = now / 1_000_000L
        if (wheel.size > 0) wheel.tick(cachedNowMs)
        if (timers.isEmpty()) return
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
        while (true) {
            if (resumeCount > 0) {
                val i = resumeHead
                val c = resumeConts[i]!!; val e = resumeErrs[i]
                resumeConts[i] = null; resumeErrs[i] = null
                resumeHead = (i + 1) % resumeConts.size; resumeCount--
                // A raw (uninterceptd) continuation: runs the coroutine's state machine right here,
                // which is correct because this *is* its dispatcher's thread (SPEC §19.3).
                if (e == null) c.resumeWith(Result.success(Unit)) else c.resumeWith(Result.failure(e))
            } else if (intCount > 0) {
                val i = intHead
                val c = intConts[i]!!; val v = intVals[i]; val e = intErrs[i]
                intConts[i] = null; intErrs[i] = null
                intHead = (i + 1) % intConts.size; intCount--
                // Pre-boxed value (SPEC §24): resuming with a fresh Integer per read/write was a heap
                // allocation per request.
                @Suppress("UNCHECKED_CAST")
                if (e == null) (c as kotlin.coroutines.Continuation<Any?>).resumeWith(Result.success(boxedInt(v)))
                else c.resumeWith(Result.failure(e))
            } else if (anyCount > 0) {
                val i = anyHead
                val c = anyConts[i]!!; val v = anyVals[i]; val e = anyErrs[i]
                anyConts[i] = null; anyVals[i] = null; anyErrs[i] = null
                anyHead = (i + 1) % anyConts.size; anyCount--
                if (e == null) c.resumeWith(Result.success(v)) else c.resumeWith(Result.failure(e))
            } else if (tasks.isNotEmpty()) {
                tasks.removeFirst().run()
            } else break
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

    protected fun hasTasks(): Boolean = resumeCount > 0 || intCount > 0 || anyCount > 0 || tasks.isNotEmpty()

    /**
     * Read available bytes into [dst]; returns the count (>0) or -1 at EOF. Reactor thread only.
     * [sizer] gives the read size and hears about each successful read (SPEC §24: the stream needs
     * no work after this call, so its read is a tail call and allocates nothing).
     */
    abstract suspend fun read(fd: Int, dst: Buffer, sizer: ReadSizer): Int

    /** Write all readable bytes from [src]; returns the number written. */
    abstract suspend fun write(fd: Int, src: Buffer): Int

    /** Vectored write of `bufs[0 until count]` (SPEC §23.3); drivers override with one syscall / SQE per batch. */
    open suspend fun writev(fd: Int, bufs: Array<Buffer>, count: Int): Long {
        var total = 0L
        for (i in 0 until count) if (bufs[i].readableBytes > 0) total += write(fd, bufs[i])
        return total
    }

    /** Half-close the write side (SPEC §23.3). A plain syscall in every driver: writes already returned are done. */
    open fun shutdownOutput(fd: Int) {
        checkOwner("shutdownOutput")
        shutdownWrite(fd)
    }

    /** Accept one connection, returning a non-blocking client fd. */
    abstract suspend fun accept(listenFd: Int): Int

    /** Wait for a non-blocking connect on [fd] to complete. */
    abstract suspend fun awaitConnect(fd: Int)

    /** Drive the loop until [root] completes. */
    abstract fun runUntil(root: Job)

    abstract fun shutdown()

    companion object {
        const val DEFAULT_TASK_BUDGET = 256
        internal const val RUNNING = 0
        internal const val DRAINING = 1
        internal const val CLOSING = 2
        internal const val STOPPED = 3
        private val CLOSED_ENTRY = ExtNode(Runnable { }, null)

        /**
         * Run a reactor on the calling thread as a long-lived worker: [ready] receives the reactor
         * and its root scope once the loop is up (so another thread can dispatch work to it), and
         * the loop runs until [stop] is completed. Used by [ReactorGroup] for one-reactor-per-core.
         */
        fun runServing(stop: kotlinx.coroutines.CompletableDeferred<Unit>, ready: (Reactor, CoroutineScope) -> Unit) {
            val reactor = createReactor()
            val scope = CoroutineScope(reactor)
            val job = scope.launch(start = CoroutineStart.DEFAULT) {
                ready(reactor, this)
                stop.await()
            }
            reactor.bindOwner()
            reactor.runUntil(job)
            reactor.shutdown()
            reactor.printStats()
        }

        fun run(block: suspend CoroutineScope.() -> Unit) {
            val reactor = createReactor()
            // Like runBlocking: the block and everything it starts form one scope. A failure anywhere
            // in it (the block, or a child) cancels the rest and is rethrown here once the loop ends.
            // As `async` the failure stays in the Deferred instead of reaching the uncaught-exception
            // handler (which aborted the process); a failed block no longer leaves parked children
            // waiting forever.
            val root = CoroutineScope(reactor).async(start = CoroutineStart.DEFAULT) { block() }
            statsReactor = reactor
            reactor.bindOwner()
            reactor.runUntil(root)
            reactor.shutdown()
            reactor.printStats()
            statsReactor = null
            root.getCompletionExceptionOrNull()?.let { throw it }
        }
    }
}

/** Create the reactor for this platform, honoring NETON_IO_DRIVER. */
internal expect fun createReactor(): Reactor

/** Read sizing for [Reactor.read]: the size to ask for, and a callback after each successful read. */
internal interface ReadSizer {
    fun readChunk(): Int
    fun onRead(n: Int)
}

/** A fixed read size (tests, internal callers without adaptive sizing). */
internal class FixedReadSize(private val size: Int) : ReadSizer {
    override fun readChunk(): Int = size
    override fun onRead(n: Int) {}
}

private val intBoxes = arrayOfNulls<Any>(65537)

/**
 * [v] as a shared boxed Int for 0..65536 (SPEC §24), created once and reused, so resuming a
 * continuation with a byte count allocates nothing. Racy first stores are harmless: equal values.
 */
internal fun boxedInt(v: Int): Any {
    if (v < 0 || v > 65536) return v
    return intBoxes[v] ?: (v as Any).also { intBoxes[v] = it }
}

/**
 * Return [n] from a suspend function without allocating (SPEC §24). A suspend function's result is
 * an `Any?` at the ABI level, so `return n` boxes every Int outside -128..127 — one heap object per
 * read or write. Returning through the intrinsic hands the shared box from [boxedInt] up the tail
 * calls unchanged; the caller's state machine unboxes it.
 */
@Suppress("NOTHING_TO_INLINE")
internal suspend inline fun intResult(n: Int): Int =
    kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn { boxedInt(n) }

/** [neton.io.core.IoStream] over a fd, delegating every operation to the [Reactor]. */
internal class ReactorStream(
    internal val fd: Int,
    private val reactor: Reactor,
    private val maxReadChunk: Int = 64 * 1024,
) : WheelNode(), neton.io.core.IoStream, ReadSizer {
    // SPEC §28.6: sockets (TCP and Unix) support half-close, all three timeouts and cancellation
    // without losing data; they belong to their reactor's thread.
    override val capabilities: Set<neton.io.core.StreamCapability> get() = SOCKET_STREAM_CAPABILITIES

    private var closed = false

    // SPEC §23.2 timeouts (ms, 0 = off) and the deadlines they produce, in reactor-clock ms.
    private var readTimeoutMs = 0L
    private var writeTimeoutMs = 0L
    private var idleTimeoutMs = 0L
    private var readDeadline = 0L
    private var writeDeadline = 0L
    private var lastActivityMs = 0L

    override fun setTimeouts(readTimeoutMillis: Long, writeTimeoutMillis: Long, idleTimeoutMillis: Long) {
        reactor.checkOwnerPublic("setTimeouts")
        readTimeoutMs = readTimeoutMillis.coerceAtLeast(0); writeTimeoutMs = writeTimeoutMillis.coerceAtLeast(0)
        idleTimeoutMs = idleTimeoutMillis.coerceAtLeast(0)
        if (idleTimeoutMs > 0) {
            lastActivityMs = reactor.timeoutClockMs()
            reactor.wheel.schedule(this, lastActivityMs + idleTimeoutMs)
        }
    }

    override fun setReadTimeout(millis: Long) { readTimeoutMs = millis.coerceAtLeast(0) }

    /** The wheel reached us: fire whatever expired, then report the next deadline (0 = none). */
    override fun onWheelDue(nowMs: Long): Long {
        if (closed) return 0
        if (idleTimeoutMs > 0 && lastActivityMs + idleTimeoutMs <= nowMs) {
            reactor.timeoutParked(fd, reads = true, writes = true, neton.io.core.TimeoutException("idle for $idleTimeoutMs ms"))
            readDeadline = 0; writeDeadline = 0
            closed = true
            reactor.closeStream(fd)
            return 0
        }
        val readDue = readDeadline in 1..nowMs
        val writeDue = writeDeadline in 1..nowMs
        if (readDue || writeDue) {
            // Clear before waking: the resumed operation must not be timed out a second time.
            if (readDue) readDeadline = 0
            if (writeDue) writeDeadline = 0
            reactor.timeoutParked(fd, readDue, writeDue, neton.io.core.TimeoutException(if (readDue) "read timed out" else "write timed out"))
        }
        var next = Long.MAX_VALUE
        if (readDeadline > nowMs) next = minOf(next, readDeadline)
        if (writeDeadline > nowMs) next = minOf(next, writeDeadline)
        if (idleTimeoutMs > 0) next = minOf(next, lastActivityMs + idleTimeoutMs)
        return if (next == Long.MAX_VALUE) 0 else next
    }

    // SPEC §19.4: adaptive read size. Reserving the maximum on every read made every buffer a
    // connection reads into grow to 64-128 KB on its first read and keep it for life. Start small,
    // double when a read fills what it was offered, halve after two consecutive reads that used
    // less than a quarter. `reserve` still offers all free space the buffer already has.
    private var readGuess = MIN_READ_CHUNK
    private var smallReads = 0

    init { reactor.registerStream(fd) }

    // The adaptive size is driven by the reactor through [ReadSizer] (SPEC §24).

    override fun readChunk(): Int = readGuess

    override fun onRead(n: Int) {
        if (n >= readGuess) {
            if (readGuess < maxReadChunk) readGuess = (readGuess * 2).coerceAtMost(maxReadChunk)
            smallReads = 0
        } else if (n < readGuess / 4 && readGuess > MIN_READ_CHUNK) {
            if (++smallReads >= 2) { readGuess /= 2; smallReads = 0 }
        } else smallReads = 0
    }

    // No timeouts (the default): a tail call straight into the reactor — no state machine, no
    // continuation allocated per read (SPEC §23.2, §24).
    override suspend fun read(dst: Buffer): Int {
        if (closed) throw neton.io.core.ClosedException()
        reactor.checkOwnerPublic("read")
        if (readTimeoutMs == 0L && idleTimeoutMs == 0L) return reactor.read(fd, dst, this)
        return timedRead(dst)
    }

    private suspend fun timedRead(dst: Buffer): Int {
        if (readTimeoutMs > 0) { readDeadline = reactor.timeoutClockMs() + readTimeoutMs; reactor.wheel.schedule(this, readDeadline) }
        val n = try { reactor.read(fd, dst, this) } finally { readDeadline = 0 }
        if (idleTimeoutMs > 0) lastActivityMs = reactor.timeoutClockMs()
        return n
    }

    private companion object { const val MIN_READ_CHUNK = 2 * 1024 }

    // Without timeouts write/writev end in a tail call, so they compile without a state machine and
    // allocate no continuation per call (SPEC §23.2: timeouts off costs nothing on the hot path).
    override suspend fun write(src: Buffer): Int {
        if (closed) throw neton.io.core.ClosedException()
        reactor.checkOwnerPublic("write")
        if (writeTimeoutMs == 0L && idleTimeoutMs == 0L) return reactor.write(fd, src)
        return timedWrite(src)
    }

    private suspend fun timedWrite(src: Buffer): Int {
        if (writeTimeoutMs > 0) { writeDeadline = reactor.timeoutClockMs() + writeTimeoutMs; reactor.wheel.schedule(this, writeDeadline) }
        val n = try { reactor.write(fd, src) } finally { writeDeadline = 0 }
        if (idleTimeoutMs > 0) lastActivityMs = reactor.timeoutClockMs()
        return n
    }

    override suspend fun writev(buffers: Array<Buffer>, count: Int): Long {
        if (closed) throw neton.io.core.ClosedException()
        reactor.checkOwnerPublic("writev")
        require(count in 0..buffers.size) { "count $count out of 0..${buffers.size}" }
        if (writeTimeoutMs == 0L && idleTimeoutMs == 0L) return reactor.writev(fd, buffers, count)
        return timedWritev(buffers, count)
    }

    private suspend fun timedWritev(buffers: Array<Buffer>, count: Int): Long {
        if (writeTimeoutMs > 0) { writeDeadline = reactor.timeoutClockMs() + writeTimeoutMs; reactor.wheel.schedule(this, writeDeadline) }
        val n = try { reactor.writev(fd, buffers, count) } finally { writeDeadline = 0 }
        if (idleTimeoutMs > 0) lastActivityMs = reactor.timeoutClockMs()
        return n
    }

    override suspend fun shutdownOutput() {
        if (closed) throw neton.io.core.ClosedException()
        reactor.shutdownOutput(fd)
    }

    override suspend fun flush() {}

    override fun close() {
        if (closed) return
        // Ownership is checked *before* any state changes (P1-2). The reverse order left a
        // cross-thread close half-applied: closeStream() threw, but `closed` was already true,
        // so the stream was permanently unusable and its fd never closed — a silent leak.
        reactor.checkOwnerPublic("close")
        closed = true
        reactor.wheel.cancel(this)
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

/**
 * Work was posted to a reactor that no longer accepts it (SPEC §28.3): it is closing or stopped. Only
 * coroutines outside the reactor's scope can hit this, which is a lifecycle bug in the caller.
 */
class ReactorStoppedException(message: String) : IllegalStateException(message)

private val SOCKET_STREAM_CAPABILITIES: Set<neton.io.core.StreamCapability> = setOf(
    neton.io.core.StreamCapability.HalfClose, neton.io.core.StreamCapability.ReadTimeout,
    neton.io.core.StreamCapability.WriteTimeout, neton.io.core.StreamCapability.IdleTimeout,
    neton.io.core.StreamCapability.ResumableAfterCancel,
)
