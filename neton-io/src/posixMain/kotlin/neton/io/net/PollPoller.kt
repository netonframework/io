package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import platform.posix.POLLERR
import platform.posix.POLLHUP
import platform.posix.POLLNVAL
import platform.posix.POLLIN
import platform.posix.POLLOUT
import platform.posix.pollfd

/**
 * Portable poll(2) [Poller]. Readiness-based like epoll/kqueue but O(n) per call; it is the
 * baseline/fallback driver and works anywhere POSIX poll does.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PollPoller : Poller {
    override val name: String get() = "polling"

    private val readFds = HashSet<Int>()
    private val writeFds = HashSet<Int>()

    override fun armRead(fd: Int) { readFds.add(fd) }
    override fun armWrite(fd: Int) { writeFds.add(fd) }
    // poll(2) has no registration: a closed fd left in the set would report POLLNVAL forever.
    override fun forget(fd: Int) { readFds.remove(fd); writeFds.remove(fd) }

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
            pollFds(arr, m, timeoutMillis)
            var count = 0
            for (i in 0 until m) {
                val re = arr[i].revents.toInt()
                val fd = fds[i]
                val err = (re and (POLLERR or POLLHUP or POLLNVAL)) != 0
                // An error or hang-up wakes whichever side is armed; the woken call then sees the
                // actual outcome (SO_ERROR for a connect, a failing send/recv). Reporting it as
                // readable only left a parked writer — e.g. a connect refused on macOS, where
                // poll() gives POLLHUP without POLLOUT — asleep forever, while the fd stayed in the
                // write set and poll() kept returning it at once: a hang at 100% CPU.
                val readable = (re and POLLIN) != 0 || (err && fd in readFds)
                val writable = (re and POLLOUT) != 0 || (err && fd in writeFds)
                if (readable || writable) {
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

/**
 * poll(2) behind a platform seam. `nfds_t` is 32-bit on Apple and 64-bit on Linux; the shared
 * native source set is compiled once against the commonized libc, which cannot express a parameter
 * whose width differs per platform, so the call itself lives in the per-platform source sets.
 */
@OptIn(ExperimentalForeignApi::class)
internal expect fun pollFds(fds: CPointer<pollfd>, count: Int, timeoutMillis: Int): Int
