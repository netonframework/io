package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.ptr
import platform.darwin.EVFILT_READ
import platform.darwin.EVFILT_WRITE
import platform.darwin.EV_ADD
import platform.darwin.EV_CLEAR
import platform.darwin.EV_EOF
import platform.darwin.EV_ERROR
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

    // Pending changes, submitted together with the next poll() (kqueue changelist). Primitive
    // arrays: registering interest boxes nothing (SPEC §24).
    private var changeFds = IntArray(64)
    private var changeFilters = ShortArray(64)
    private var changeFlags = IntArray(64)
    private var nChanges = 0
    private val oneShot: Int = EV_ADD or EV_ONESHOT
    private val edge: Int = EV_ADD or EV_CLEAR

    private fun change(fd: Int, filter: Short, flags: Int) {
        if (nChanges == changeFds.size) {
            changeFds = changeFds.copyOf(nChanges * 2); changeFilters = changeFilters.copyOf(nChanges * 2); changeFlags = changeFlags.copyOf(nChanges * 2)
        }
        changeFds[nChanges] = fd; changeFilters[nChanges] = filter; changeFlags[nChanges] = flags; nChanges++
    }

    override val persistentRead: Boolean get() = true

    /** Persistent, edge-triggered read interest: registered once, never re-armed. */
    override fun watchRead(fd: Int) = change(fd, EVFILT_READ.toShort(), edge)

    override fun armRead(fd: Int) = change(fd, EVFILT_READ.toShort(), oneShot)

    override fun armWrite(fd: Int) = change(fd, EVFILT_WRITE.toShort(), oneShot)

    // A closed fd is removed from the kqueue automatically; only the unsubmitted changelist can
    // still name it (and its number may be reused before the next poll), so drop those entries.
    override fun forget(fd: Int) {
        var w = 0
        for (i in 0 until nChanges) {
            if (changeFds[i] == fd) continue
            changeFds[w] = changeFds[i]; changeFilters[w] = changeFilters[i]; changeFlags[w] = changeFlags[i]; w++
        }
        nChanges = w
    }

    // SPEC §24: kevent buffers live as long as the poller; poll() allocates nothing.
    private val maxEvents = 64
    private val events = nativeHeap.allocArray<kevent>(maxEvents)
    private var changes = nativeHeap.allocArray<kevent>(64)
    private var changesCap = 64
    private val ts = nativeHeap.alloc<timespec>()
    private val ready = ReadyEvents(maxEvents)

    override fun poll(timeoutMillis: Int): Int {
        val nc = nChanges
        if (nc > changesCap) {
            nativeHeap.free(changes.rawValue)
            changesCap = maxOf(nc, changesCap * 2)
            changes = nativeHeap.allocArray(changesCap)
        }
        for (i in 0 until nc) {
            val ev = changes[i]
            ev.ident = changeFds[i].convert()
            ev.filter = changeFilters[i]
            ev.flags = changeFlags[i].toUShort()
            ev.fflags = 0u
            ev.data = 0
            ev.udata = null
        }
        nChanges = 0
        val n = if (timeoutMillis < 0) {
            kevent(kq, changes, nc, events, maxEvents, null)
        } else {
            ts.tv_sec = (timeoutMillis / 1000).convert()
            ts.tv_nsec = ((timeoutMillis % 1000) * 1_000_000).convert()
            kevent(kq, changes, nc, events, maxEvents, ts.ptr)
        }
        ready.reset()
        for (i in 0 until n) {
            val ev = events[i]
            var f = 0
            if (ev.filter == EVFILT_READ.toShort()) f = f or READY_READ
            if (ev.filter == EVFILT_WRITE.toShort()) f = f or READY_WRITE
            if ((ev.flags.toInt() and (EV_EOF or EV_ERROR)) != 0) f = f or READY_HUP
            ready.add(ev.ident.toInt(), f)
        }
        return ready.count
    }

    override fun readyFd(i: Int): Int = ready.fds[i]
    override fun readyFlags(i: Int): Int = ready.flags[i]

    override fun close() {
        nativeHeap.free(events.rawValue); nativeHeap.free(changes.rawValue); nativeHeap.free(ts.rawPtr)
        close(kq)
    }
}

/** Apple uses kqueue (poll(2) selectable via NETON_IO_DRIVER=polling). */
internal actual fun createPoller(): Poller =
    if (driverSelection() == "polling") PollPoller() else KqueuePoller()
