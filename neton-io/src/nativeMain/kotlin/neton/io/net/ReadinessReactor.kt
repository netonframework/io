package neton.io.net

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Job
import kotlinx.coroutines.suspendCancellableCoroutine
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import platform.posix.errno
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Readiness reactor backed by a [Poller] (kqueue/epoll/poll). A read waits for the fd to be
 * readable and then does the recv; the reactor blocks in the poller when idle (no busy poll).
 */
internal class ReadinessReactor(private val poller: Poller) : Reactor() {

    override val driverName: String get() = poller.name

    private val readWaiters = HashMap<Int, CancellableContinuation<Unit>>()
    private val writeWaiters = HashMap<Int, CancellableContinuation<Unit>>()

    // SPEC §17: fds with persistent edge-triggered read interest, and those with an unconsumed
    // readable edge (event arrived with nobody parked). A read on such an fd recv()s until EAGAIN,
    // then clears the flag and parks; it never recv()s speculatively.
    private val persistent = HashSet<Int>()
    private val readyRead = HashSet<Int>()

    override fun registerStream(fd: Int) {
        if (poller.persistentRead) { persistent.add(fd); poller.watchRead(fd) }
    }

    private suspend fun waitReadable(fd: Int): Unit = suspendCancellableCoroutine { cont ->
        readWaiters[fd] = cont
        cont.invokeOnCancellation { postToReactor { readWaiters.remove(fd) } }
        if (fd !in persistent) poller.armRead(fd)
    }

    private suspend fun waitWritable(fd: Int): Unit = suspendCancellableCoroutine { cont ->
        writeWaiters[fd] = cont
        cont.invokeOnCancellation { postToReactor { writeWaiters.remove(fd) } }
        poller.armWrite(fd)
    }

    override suspend fun read(fd: Int, dst: Buffer, chunk: Int): Int {
        while (true) {
            if (fd in persistent && fd !in readyRead) { waitReadable(fd); continue }
            val outcome = readInto(fd, dst, chunk)
            stats?.let { it.reads++; if (outcome.result == IoResult.OK) it.readBytes += outcome.count else if (outcome.result == IoResult.WOULD_BLOCK) it.readsWouldBlock++ }
            when (outcome.result) {
                IoResult.OK -> {
                    // SPEC §17b short-read rule: a recv that returned less than it was offered has
                    // emptied the socket, so clear the ready flag now instead of paying a second
                    // recv for the EAGAIN. Safe under EPOLLET/EV_CLEAR: any byte arriving after
                    // this recv raises a fresh edge, so the next read() will not park on it.
                    if (outcome.count < outcome.requested) readyRead.remove(fd)
                    return outcome.count
                }
                IoResult.EOF -> return -1
                IoResult.ERROR -> { val e = errno; throw IoException("read failed: ${errnoMessage(e)}", e) }
                IoResult.WOULD_BLOCK -> { readyRead.remove(fd); waitReadable(fd) }
            }
        }
    }

    override suspend fun write(fd: Int, src: Buffer): Int {
        var total = 0
        while (src.readableBytes > 0) {
            val n = writeFrom(fd, src)
            stats?.let { it.writes++; if (n >= 0) it.writeBytes += n else if (n == WOULD_BLOCK) it.writesWouldBlock++ }
            when {
                n >= 0 -> total += n
                n == WOULD_BLOCK -> waitWritable(fd)
                else -> { val e = errno; throw IoException("write failed: ${errnoMessage(e)}", e) }
            }
        }
        return total
    }

    override suspend fun accept(listenFd: Int): Int {
        while (true) {
            val clientFd = acceptOne(listenFd)
            if (clientFd >= 0) {
                setNonBlocking(clientFd)
                suppressSigpipe(clientFd)
                return clientFd
            }
            waitReadable(listenFd)
        }
    }

    override suspend fun awaitConnect(fd: Int) = waitWritable(fd)

    override fun runUntil(root: Job) {
        poller.armRead(wakeReadFd)
        while (!root.isCompleted) {
            absorbExternal()
            fireTimers()
            drainTasks()
            if (root.isCompleted) break

            val timeout = if (hasTasks()) 0 else nextTimerMillis()
            var woke = false
            val n = poller.poll(timeout) { fd, readable, writable ->
                if (fd == wakeReadFd) { woke = true; return@poll }
                if (readable) {
                    // Persistent fds: record the edge *before* resuming, so the resumed read() sees
                    // the fd as ready instead of parking again (the edge is not repeated).
                    if (fd in persistent) readyRead.add(fd)
                    readWaiters.remove(fd)?.resume(Unit)
                }
                if (writable) writeWaiters.remove(fd)?.resume(Unit)
            }
            if (woke) { onWake(); poller.armRead(wakeReadFd) }
            countPoll(timeout == 0, n)
        }
    }

    override fun closeStream(fd: Int) {
        checkOwner("close")
        readWaiters.remove(fd)?.resumeWithException(ClosedException())
        writeWaiters.remove(fd)?.resumeWithException(ClosedException())
        persistent.remove(fd); readyRead.remove(fd)
        poller.forget(fd)
        closeFd(fd)
    }

    override fun shutdown() {
        poller.close()
        closeWakePipe()
    }
}
