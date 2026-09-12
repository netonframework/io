package neton.io.net

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume

/**
 * Single-threaded reactor that doubles as the [CoroutineDispatcher] for the coroutines
 * it drives. The reactor is the scheduler: when a coroutine blocks on I/O it parks on an
 * fd; when the [Poller] reports readiness the parked continuation is resumed, which
 * re-enqueues it as a runnable task.
 *
 * The loop blocks in the poller (timeout -1) only when there are no runnable tasks, so
 * an idle connection costs no CPU — there is no busy polling.
 */
internal class EventLoop : CoroutineDispatcher() {

    private val poller = Poller()
    private val tasks = ArrayDeque<Runnable>()
    private val readWaiters = HashMap<Int, CancellableContinuation<Unit>>()
    private val writeWaiters = HashMap<Int, CancellableContinuation<Unit>>()

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        tasks.addLast(block)
    }

    /** Suspend until [fd] is readable. */
    suspend fun waitReadable(fd: Int): Unit = suspendCancellableCoroutine { cont ->
        readWaiters[fd] = cont
        cont.invokeOnCancellation { readWaiters.remove(fd) }
        poller.armRead(fd)
    }

    /** Suspend until [fd] is writable. */
    suspend fun waitWritable(fd: Int): Unit = suspendCancellableCoroutine { cont ->
        writeWaiters[fd] = cont
        cont.invokeOnCancellation { writeWaiters.remove(fd) }
        poller.armWrite(fd)
    }

    private fun runUntil(root: Job) {
        while (!root.isCompleted) {
            while (tasks.isNotEmpty()) tasks.removeFirst().run()
            if (root.isCompleted) break

            val hasWaiters = readWaiters.isNotEmpty() || writeWaiters.isNotEmpty()
            if (!hasWaiters && tasks.isEmpty()) break // nothing left to make progress

            val timeout = if (tasks.isNotEmpty()) 0 else -1
            poller.poll(timeout) { fd, readable, writable ->
                if (readable) readWaiters.remove(fd)?.resume(Unit)
                if (writable) writeWaiters.remove(fd)?.resume(Unit)
            }
        }
    }

    private fun shutdown() = poller.close()

    companion object {
        /** Run [block] on a fresh event loop until it completes. */
        fun run(block: suspend CoroutineScope.() -> Unit) {
            val loop = EventLoop()
            var failure: Throwable? = null
            val scope = CoroutineScope(loop)
            val job = scope.launch(start = CoroutineStart.DEFAULT) {
                try {
                    block()
                } catch (t: Throwable) {
                    failure = t
                }
            }
            loop.runUntil(job)
            loop.shutdown()
            failure?.let { throw it }
        }
    }
}
