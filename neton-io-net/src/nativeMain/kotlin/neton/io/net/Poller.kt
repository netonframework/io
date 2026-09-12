package neton.io.net

/**
 * Readiness poller SPI. Backends: kqueue (Apple), epoll and poll (Linux); io_uring (Linux)
 * and IOCP (Windows) are completion-based and land as separate drivers.
 *
 * Interest is one-shot: once an fd fires it is disarmed, and the waiting coroutine re-arms on
 * its next read/write. This keeps the reactor edge-driven and matches [SocketStream].
 */
internal interface Poller {
    val name: String
    fun armRead(fd: Int)
    fun armWrite(fd: Int)

    /**
     * Block for up to [timeoutMillis] (-1 until an event, 0 to return immediately), calling
     * [onReady] once per ready fd. Returns the number of events.
     */
    fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int

    fun close()
}

/** Create the poller for this platform, honoring the NETON_IO_DRIVER selection where applicable. */
internal expect fun createPoller(): Poller
