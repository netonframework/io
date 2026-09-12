package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import platform.posix.POLLERR
import platform.posix.POLLHUP
import platform.posix.POLLIN
import platform.posix.POLLOUT
import platform.posix.getenv
import platform.posix.poll
import platform.posix.pollfd

/** Selected driver name from NETON_IO_DRIVER (lowercased), or null. */
@OptIn(ExperimentalForeignApi::class)
internal fun driverSelection(): String? = getenv("NETON_IO_DRIVER")?.toKString()?.lowercase()

/**
 * Portable poll(2) [Poller]. Readiness-based like epoll/kqueue but O(n) per call; it is the
 * baseline/fallback driver and works anywhere POSIX poll does.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PollPoller : Poller {

    private val readFds = HashSet<Int>()
    private val writeFds = HashSet<Int>()

    override fun armRead(fd: Int) { readFds.add(fd) }
    override fun armWrite(fd: Int) { writeFds.add(fd) }

    override fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int {
        val fds = IntArray(readFds.size + writeFds.size)
        var m = 0
        val union = HashSet<Int>(readFds.size + writeFds.size)
        for (fd in readFds) if (union.add(fd)) fds[m++] = fd
        for (fd in writeFds) if (union.add(fd)) fds[m++] = fd
        if (m == 0) return 0

        return memScoped {
            val arr = allocArray<pollfd>(m)
            for (i in 0 until m) {
                val fd = fds[i]
                var ev = 0
                if (fd in readFds) ev = ev or POLLIN
                if (fd in writeFds) ev = ev or POLLOUT
                arr[i].fd = fd
                arr[i].events = ev.toShort()
                arr[i].revents = 0
            }
            poll(arr, m.convert(), timeoutMillis)
            var count = 0
            for (i in 0 until m) {
                val re = arr[i].revents.toInt()
                val err = (re and (POLLERR or POLLHUP)) != 0
                val readable = (re and POLLIN) != 0 || err
                val writable = (re and POLLOUT) != 0
                if (readable || writable) {
                    val fd = fds[i]
                    if (readable) readFds.remove(fd)
                    if (writable) writeFds.remove(fd)
                    onReady(fd, readable, writable)
                    count++
                }
            }
            count
        }
    }

    override fun close() {}
}
