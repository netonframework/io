package neton.io.net

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Job
import kotlinx.coroutines.suspendCancellableCoroutine
import neton.io.bytes.Buffer
import kotlin.coroutines.resume

/**
 * Readiness reactor backed by a [Poller] (kqueue/epoll/poll). A read waits for the fd to be
 * readable and then does the recv; the reactor blocks in the poller when idle (no busy poll).
 */
internal class ReadinessReactor(private val poller: Poller) : Reactor() {

    private val readWaiters = HashMap<Int, CancellableContinuation<Unit>>()
    private val writeWaiters = HashMap<Int, CancellableContinuation<Unit>>()

    private suspend fun waitReadable(fd: Int): Unit = suspendCancellableCoroutine { cont ->
        readWaiters[fd] = cont
        cont.invokeOnCancellation { readWaiters.remove(fd) }
        poller.armRead(fd)
    }

    private suspend fun waitWritable(fd: Int): Unit = suspendCancellableCoroutine { cont ->
        writeWaiters[fd] = cont
        cont.invokeOnCancellation { writeWaiters.remove(fd) }
        poller.armWrite(fd)
    }

    override suspend fun read(fd: Int, dst: Buffer, chunk: Int): Int {
        while (true) {
            val outcome = readInto(fd, dst, chunk)
            when (outcome.result) {
                IoResult.OK -> return outcome.count
                IoResult.EOF, IoResult.ERROR -> return -1
                IoResult.WOULD_BLOCK -> waitReadable(fd)
            }
        }
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        var total = 0
        while (src.readableBytes > 0) {
            val n = writeFrom(fd, src)
            when {
                n >= 0 -> total += n
                n == WOULD_BLOCK -> waitWritable(fd)
                else -> break // IO_ERROR
            }
        }
        return total
    }

    override suspend fun accept(listenFd: Int): Int {
        while (true) {
            val clientFd = acceptOne(listenFd)
            if (clientFd >= 0) {
                setNonBlocking(clientFd)
                return clientFd
            }
            waitReadable(listenFd)
        }
    }

    override suspend fun awaitConnect(fd: Int) = waitWritable(fd)

    override fun runUntil(root: Job) {
        while (!root.isCompleted) {
            drainTasks()
            if (root.isCompleted) break

            val hasWaiters = readWaiters.isNotEmpty() || writeWaiters.isNotEmpty()
            if (!hasWaiters && !hasTasks()) break

            val timeout = if (hasTasks()) 0 else -1
            poller.poll(timeout) { fd, readable, writable ->
                if (readable) readWaiters.remove(fd)?.resume(Unit)
                if (writable) writeWaiters.remove(fd)?.resume(Unit)
            }
        }
    }

    override fun shutdown() = poller.close()
}
