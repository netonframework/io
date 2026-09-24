package neton.io.net

import neton.io.core.IoStream
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.coroutineContext

/** The [Reactor] driving the current coroutine (it is the dispatcher). */
internal suspend fun currentReactor(): Reactor = coroutineContext[ContinuationInterceptor] as Reactor

/** A listening TCP endpoint. */
internal class TcpServer(private val listenFd: Int, private val reactor: Reactor) {
    private var closed = false

    suspend fun accept(): IoStream = ReactorStream(reactor.accept(listenFd), reactor)

    /** Accept and return the raw client fd without binding it to this reactor (for hand-off). */
    suspend fun acceptFd(): Int = reactor.accept(listenFd)

    /**
     * Close the listener the way a stream is closed: on the reactor thread, waking any coroutine
     * parked in [accept] with [neton.io.core.ClosedException] and dropping the driver's interest
     * in the fd before closing it.
     *
     * A bare closeFd() left an accept parked forever with no fd to wake it, left the poller
     * holding a registration for a descriptor number the kernel was free to hand out again, and
     * was not idempotent — a second close could take out whatever had since reused the number.
     */
    fun close() {
        if (closed) return
        reactor.checkOwnerPublic("close")
        closed = true
        reactor.closeStream(listenFd)
    }
}

/** Bind and listen on [host]:[port]. Must run inside a reactor. */
internal suspend fun listenTcpServer(host: String, port: Int): TcpServer =
    TcpServer(tcpListen(host, port), currentReactor())

/** Connect to [host]:[port], completing the non-blocking connect through the reactor. */
internal suspend fun connectStream(host: String, port: Int): IoStream {
    val reactor = currentReactor()
    val fd = tcpConnect(host, port)
    reactor.awaitConnect(fd)
    // A failed non-blocking connect also reports "writable"; the outcome is in SO_ERROR.
    val err = socketError(fd)
    if (err != 0) {
        closeFd(fd)
        throw ConnectException("connect to $host:$port failed: ${errnoMessage(err)} (errno $err)")
    }
    return ReactorStream(fd, reactor)
}
