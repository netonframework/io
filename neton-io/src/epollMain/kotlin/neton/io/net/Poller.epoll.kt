package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import platform.linux.EPOLLERR
import platform.linux.EPOLLET
import platform.linux.EPOLLHUP
import platform.linux.EPOLLIN
import platform.linux.EPOLLONESHOT
import platform.linux.EPOLLOUT
import platform.linux.EPOLL_CTL_ADD
import platform.linux.EPOLL_CTL_MOD
import platform.linux.epoll_create1
import platform.linux.epoll_ctl
import platform.linux.epoll_event
import platform.linux.epoll_wait
import platform.posix.EEXIST
import platform.posix.close
import platform.posix.errno

/** epoll-backed [Poller] for Linux targets. */
@OptIn(ExperimentalForeignApi::class)
internal class EpollPoller : Poller {
    override val name: String get() = "epoll"

    private val epfd: Int = epoll_create1(0)

    /** fds registered with persistent edge-triggered read interest (SPEC §17). */
    private val edgeFds = HashSet<Int>()
    /** of those, fds that also have persistent edge-triggered write interest. */
    private val edgeWriteFds = HashSet<Int>()

    override val persistentRead: Boolean get() = true

    override fun watchRead(fd: Int) {
        edgeFds.add(fd)
        ctl(fd, EPOLLIN.toInt() or EPOLLET.toInt())
    }

    override fun armRead(fd: Int) = arm(fd, EPOLLIN.toInt())

    // On a persistent fd, EPOLL_CTL_MOD replaces the whole interest set, so a one-shot write
    // arm would drop the read interest. Register write interest edge-triggered and persistent
    // instead (once); a writable edge with nobody waiting is simply ignored by the reactor.
    override fun armWrite(fd: Int) {
        if (fd in edgeFds) {
            if (edgeWriteFds.add(fd)) ctl(fd, EPOLLIN.toInt() or EPOLLOUT.toInt() or EPOLLET.toInt())
        } else arm(fd, EPOLLOUT.toInt())
    }

    private fun ctl(fd: Int, events: Int) = memScoped {
        val ev = alloc<epoll_event>()
        ev.events = events.convert()
        ev.data.fd = fd
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, ev.ptr) != 0 && errno == EEXIST) {
            epoll_ctl(epfd, EPOLL_CTL_MOD, fd, ev.ptr)
        }
        Unit
    }

    // epoll arms immediately (one epoll_ctl per interest), unlike kqueue's batched changelist.
    // We do not track which fds are registered: a closed fd is auto-removed from the epoll set,
    // and its number may be reused, so ADD then fall back to MOD on EEXIST is the robust path.
    private fun arm(fd: Int, events: Int) = memScoped {
        val ev = alloc<epoll_event>()
        ev.events = (events or EPOLLONESHOT.toInt()).convert()
        ev.data.fd = fd
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, ev.ptr) != 0 && errno == EEXIST) {
            epoll_ctl(epfd, EPOLL_CTL_MOD, fd, ev.ptr)
        }
        Unit
    }

    // epoll drops a closed fd from the set by itself; interest is armed immediately, so nothing is pending.
    override fun forget(fd: Int) { edgeFds.remove(fd); edgeWriteFds.remove(fd) }

    override fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int = memScoped {
        val maxEvents = 64
        val events = allocArray<epoll_event>(maxEvents)
        val n = epoll_wait(epfd, events, maxEvents, timeoutMillis)
        var count = 0
        for (i in 0 until n) {
            val ev = events[i]
            val fd = ev.data.fd
            val e = ev.events.toInt()
            // EPOLLERR / EPOLLHUP wake both sides (same rule as the poll(2) driver): the woken
            // recv/send/SO_ERROR reports the actual outcome. With nothing parked, a readable edge
            // only marks the fd ready and a writable one is ignored, so this is harmless.
            val err = (e and (EPOLLERR.toInt() or EPOLLHUP.toInt())) != 0
            val readable = (e and EPOLLIN.toInt()) != 0 || err
            val writable = (e and EPOLLOUT.toInt()) != 0 || err
            onReady(fd, readable, writable)
            count++
        }
        count
    }

    override fun close() {
        close(epfd)
    }
}

/** Linux defaults to epoll; NETON_IO_DRIVER=polling selects poll(2). io_uring lands as a driver next. */
internal actual fun createPoller(): Poller =
    when (driverSelection()) {
        "polling", "poll" -> PollPoller()
        else -> EpollPoller()
    }
