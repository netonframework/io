package neton.io.net

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import kotlin.coroutines.CoroutineContext

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
internal abstract class Reactor : CoroutineDispatcher() {

    private val tasks = ArrayDeque<Runnable>()

    final override fun dispatch(context: CoroutineContext, block: Runnable) {
        tasks.addLast(block)
    }

    protected fun drainTasks() {
        while (tasks.isNotEmpty()) tasks.removeFirst().run()
    }

    protected fun hasTasks(): Boolean = tasks.isNotEmpty()

    /** Read available bytes into [dst]; returns the count (>0) or -1 at EOF. */
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
            ignoreSigpipe()
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
            reactor.runUntil(job)
            reactor.shutdown()
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
    override suspend fun read(dst: Buffer): Int = reactor.read(fd, dst, readChunk)
    override suspend fun write(src: Buffer): Int = reactor.write(fd, src)
    override suspend fun flush() {}
    override fun close() = closeFd(fd)
}
