package neton.io.net

/**
 * Readiness poller SPI. Apple targets back this with kqueue, Linux with epoll.
 *
 * Interest is one-shot: once an fd fires it is disarmed, and the waiting coroutine
 * re-arms on its next read/write. This keeps the reactor edge-driven and matches the
 * suspend read/write model in [SocketStream].
 */
internal expect class Poller() {

    /** Arm a one-shot readable interest on [fd]. */
    fun armRead(fd: Int)

    /** Arm a one-shot writable interest on [fd]. */
    fun armWrite(fd: Int)

    /**
     * Block for up to [timeoutMillis] (-1 blocks until an event; 0 returns immediately),
     * calling [onReady] once per ready fd. Returns the number of events.
     */
    fun poll(timeoutMillis: Int, onReady: (fd: Int, readable: Boolean, writable: Boolean) -> Unit): Int

    fun close()
}
