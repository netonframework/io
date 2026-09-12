package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import platform.darwin.EVFILT_READ
import platform.darwin.EVFILT_WRITE
import platform.darwin.EV_ADD
import platform.darwin.EV_ONESHOT
import platform.darwin.kevent
import platform.darwin.kqueue
import platform.posix.close
import platform.posix.timespec

/** kqueue-backed [Poller] for Apple targets. */
@OptIn(ExperimentalForeignApi::class)
internal actual class Poller actual constructor() {

    private val kq: Int = kqueue()

    // Pending one-shot changes, submitted together with the next poll() (kqueue changelist).
    private val changeFds = ArrayList<Int>()
    private val changeFilters = ArrayList<Short>()

    actual fun armRead(fd: Int) {
        changeFds.add(fd)
        changeFilters.add(EVFILT_READ.toShort())
    }

    actual fun armWrite(fd: Int) {
        changeFds.add(fd)
        changeFilters.add(EVFILT_WRITE.toShort())
    }

    actual fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int = memScoped {
        val nChanges = changeFds.size
        val changes = if (nChanges > 0) allocArray<kevent>(nChanges) else null
        for (i in 0 until nChanges) {
            val ev = changes!![i]
            ev.ident = changeFds[i].convert()
            ev.filter = changeFilters[i]
            ev.flags = (EV_ADD or EV_ONESHOT).convert()
            ev.fflags = 0u
            ev.data = 0
            ev.udata = null
        }
        changeFds.clear()
        changeFilters.clear()

        val maxEvents = 64
        val events = allocArray<kevent>(maxEvents)

        val n = if (timeoutMillis < 0) {
            kevent(kq, changes, nChanges, events, maxEvents, null)
        } else {
            val ts = alloc<timespec>()
            ts.tv_sec = (timeoutMillis / 1000).convert()
            ts.tv_nsec = ((timeoutMillis % 1000) * 1_000_000).convert()
            kevent(kq, changes, nChanges, events, maxEvents, ts.ptr)
        }

        var count = 0
        for (i in 0 until n) {
            val ev = events[i]
            val fd = ev.ident.toInt()
            val readable = ev.filter == EVFILT_READ.toShort()
            val writable = ev.filter == EVFILT_WRITE.toShort()
            onReady(fd, readable, writable)
            count++
        }
        count
    }

    actual fun close() {
        close(kq)
    }
}
