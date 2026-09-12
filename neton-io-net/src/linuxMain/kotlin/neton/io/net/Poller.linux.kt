package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
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

    override fun armRead(fd: Int) = arm(fd, EPOLLIN.toInt())

    override fun armWrite(fd: Int) = arm(fd, EPOLLOUT.toInt())

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

    override fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int = memScoped {
        val maxEvents = 64
        val events = allocArray<epoll_event>(maxEvents)
        val n = epoll_wait(epfd, events, maxEvents, timeoutMillis)
        var count = 0
        for (i in 0 until n) {
            val ev = events[i]
            val fd = ev.data.fd
            val e = ev.events.toInt()
            val readable = (e and EPOLLIN.toInt()) != 0
            val writable = (e and EPOLLOUT.toInt()) != 0
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
