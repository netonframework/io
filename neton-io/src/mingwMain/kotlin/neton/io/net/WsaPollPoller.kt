@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import platform.posix.POLLERR
import platform.posix.POLLHUP
import platform.posix.POLLNVAL
import platform.posix.POLLRDNORM
import platform.posix.POLLWRNORM
import platform.posix.WSAPoll
import platform.posix.pollfd

/**
 * WSAPoll [Poller] (SPEC §20, Windows, correctness first). Same semantics as the POSIX poll(2)
 * driver, including "an error or hang-up wakes whichever side is armed". WSAPoll rejects POLLPRI,
 * so reads ask for POLLRDNORM and writes for POLLWRNORM. The IOCP driver replaces this for
 * performance.
 */
internal class WsaPollPoller : Poller {
    override val name: String get() = "wsapoll"

    private val readFds = HashSet<Int>()
    private val writeFds = HashSet<Int>()

    override fun armRead(fd: Int) { readFds.add(fd) }
    override fun armWrite(fd: Int) { writeFds.add(fd) }
    override fun forget(fd: Int) { readFds.remove(fd); writeFds.remove(fd) }

    private val ready = ReadyEvents()
    override fun readyFd(i: Int): Int = ready.fds[i]
    override fun readyFlags(i: Int): Int = ready.flags[i]

    override fun poll(timeoutMillis: Int): Int {
        ready.reset()
        val fds = IntArray(readFds.size + writeFds.size)
        var m = 0
        val union = HashSet<Int>(readFds.size + writeFds.size)
        for (fd in readFds) if (union.add(fd)) fds[m++] = fd
        for (fd in writeFds) if (union.add(fd)) fds[m++] = fd
        if (m == 0) {
            // WSAPoll with no sockets fails instead of sleeping; honour the timeout ourselves.
            if (timeoutMillis > 0) platform.posix.usleep((timeoutMillis * 1000).toUInt())
            return 0
        }
        return memScoped {
            val arr = allocArray<pollfd>(m)
            for (i in 0 until m) {
                val fd = fds[i]
                var ev = 0
                if (fd in readFds) ev = ev or POLLRDNORM
                if (fd in writeFds) ev = ev or POLLWRNORM
                arr[i].fd = fd.toSocket()
                arr[i].events = ev.toShort()
                arr[i].revents = 0
            }
            WSAPoll(arr, m.toUInt(), timeoutMillis)
            var count = 0
            for (i in 0 until m) {
                val re = arr[i].revents.toInt()
                val fd = fds[i]
                val err = (re and (POLLERR or POLLHUP or POLLNVAL)) != 0
                val readable = (re and POLLRDNORM) != 0 || (err && fd in readFds)
                val writable = (re and POLLWRNORM) != 0 || (err && fd in writeFds)
                if (readable || writable) {
                    if (readable) readFds.remove(fd)
                    if (writable) writeFds.remove(fd)
                    ready.add(fd, (if (readable) READY_READ else 0) or (if (writable) READY_WRITE else 0) or (if (err) READY_HUP else 0))
                    count++
                }
            }
            count
        }
    }

    override fun close() {}
}
