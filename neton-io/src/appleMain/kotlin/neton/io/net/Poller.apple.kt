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
import platform.darwin.EV_CLEAR
import platform.darwin.EV_ONESHOT
import platform.darwin.kevent
import platform.darwin.kqueue
import platform.posix.close
import platform.posix.timespec

/** kqueue-backed [Poller] for Apple targets. */
@OptIn(ExperimentalForeignApi::class)
internal class KqueuePoller : Poller {
    override val name: String get() = "kqueue"

    private val kq: Int = kqueue()

    // Pending one-shot changes, submitted together with the next poll() (kqueue changelist).
    private val changeFds = ArrayList<Int>()
    private val changeFilters = ArrayList<Short>()
    private val changeFlags = ArrayList<UShort>()
    private val oneShot: UShort = (EV_ADD or EV_ONESHOT).toUShort()
    private val edge: UShort = (EV_ADD or EV_CLEAR).toUShort()

    override val persistentRead: Boolean get() = true

    /** Persistent, edge-triggered read interest: registered once, never re-armed. */
    override fun watchRead(fd: Int) {
        changeFds.add(fd); changeFilters.add(EVFILT_READ.toShort()); changeFlags.add(edge)
    }

    override fun armRead(fd: Int) {
        changeFds.add(fd); changeFilters.add(EVFILT_READ.toShort()); changeFlags.add(oneShot)
    }

    override fun armWrite(fd: Int) {
        changeFds.add(fd); changeFilters.add(EVFILT_WRITE.toShort()); changeFlags.add(oneShot)
    }

    // A closed fd is removed from the kqueue automatically; only the unsubmitted changelist can
    // still name it (and its number may be reused before the next poll), so drop those entries.
    override fun forget(fd: Int) {
        var i = changeFds.size - 1
        while (i >= 0) {
            if (changeFds[i] == fd) { changeFds.removeAt(i); changeFilters.removeAt(i); changeFlags.removeAt(i) }
            i--
        }
    }

    override fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int = memScoped {
        val nChanges = changeFds.size
        val changes = if (nChanges > 0) allocArray<kevent>(nChanges) else null
        for (i in 0 until nChanges) {
            val ev = changes!![i]
            ev.ident = changeFds[i].convert()
            ev.filter = changeFilters[i]
            ev.flags = changeFlags[i]
            ev.fflags = 0u
            ev.data = 0
            ev.udata = null
        }
        changeFds.clear()
        changeFilters.clear()
        changeFlags.clear()

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

    override fun close() {
        close(kq)
    }
}

/** Apple uses kqueue (poll(2) selectable via NETON_IO_DRIVER=polling). */
internal actual fun createPoller(): Poller =
    if (driverSelection() == "polling") PollPoller() else KqueuePoller()
