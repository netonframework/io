package neton.io.net

import kotlinx.coroutines.withTimeoutOrNull

import neton.io.core.IoStream
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.coroutineContext

/** The [Reactor] driving the current coroutine (it is the dispatcher). */
internal suspend fun currentReactor(): Reactor = coroutineContext[ContinuationInterceptor] as Reactor

/** A listening TCP endpoint. */
internal class TcpServer(
    internal val listenFd: Int,
    private val reactor: Reactor,
    /** Applied to every accepted connection (SPEC §23.5). */
    private val options: SocketOptions = SocketOptions.Default,
) {
    private var closed = false
    /** Run once after [close] (a Unix listener removes its socket file). */
    internal var afterClose: (() -> Unit)? = null

    /** A closed listener's fd number may already belong to someone else: never accept on it. */
    private fun checkOpen() { if (closed) throw neton.io.core.ClosedException("listener closed") }

    suspend fun accept(): IoStream {
        checkOpen()
        val fd = reactor.accept(listenFd)
        applyStreamOptions(fd, options)
        return ReactorStream(fd, reactor)
    }

    /** Accept and return the raw client fd without binding it to this reactor (for hand-off). */
    suspend fun acceptFd(): Int { checkOpen(); return reactor.accept(listenFd).also { applyStreamOptions(it, options) } }

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
        afterClose?.invoke()
    }
}

/**
 * Bind and listen on [host]:[port] (IPv4 or IPv6 literal, or a name; SPEC §18.2). Binds the first
 * address that works. Must run inside a reactor.
 */
internal suspend fun listenTcpServer(host: String, port: Int, options: SocketOptions = SocketOptions.Default): TcpServer {
    val display = hostPort(host, port)
    val addrs = try { resolve(host, port, passive = true) } catch (e: ResolveException) { error("bind($display) failed: ${e.message}") }
    var last: Throwable? = null
    for (a in addrs) {
        try { return TcpServer(tcpListenAddr(a, display, options), currentReactor(), options) } catch (e: IllegalStateException) { last = e }
    }
    throw last!!
}

/**
 * Connect to [host]:[port], completing the non-blocking connect through the reactor. Every
 * resolved address is tried in order (e.g. `localhost` → `::1`, then `127.0.0.1`); the last
 * failure is reported if none connects (SPEC §18.2).
 */
internal suspend fun connectStream(host: String, port: Int, options: SocketOptions = SocketOptions.Default): IoStream {
    val reactor = currentReactor()
    val display = hostPort(host, port)
    val addrs = try { resolve(host, port, passive = false) } catch (e: ResolveException) {
        throw ConnectException("connect to $display failed: ${e.message}")
    }
    var last: ConnectException? = null
    for (a in addrs) {
        val fd = try { tcpConnectAddr(a, display, options) } catch (e: ConnectException) { last = e; continue }
        try { return finishConnect(reactor, fd, display, options) } catch (e: ConnectException) { last = e }
    }
    throw last ?: ConnectException("connect to $display failed: no addresses")
}

/**
 * Complete a non-blocking connect started on [fd]: wait for writability (bounded by the connect
 * timeout, SPEC §23.5), then read the outcome from SO_ERROR. Throws [ConnectException] and closes
 * [fd] on failure.
 */
internal suspend fun finishConnect(reactor: Reactor, fd: Int, display: String, options: SocketOptions): IoStream {
    if (options.connectTimeoutMillis > 0) {
        // On expiry the half-open socket is closed through the reactor (drops the driver's
        // interest; a bare close would leave poll(2) spinning on it).
        val done = withTimeoutOrNull(options.connectTimeoutMillis) { reactor.awaitConnect(fd); true }
        if (done == null) {
            reactor.closeStream(fd)
            throw ConnectException("connect to $display timed out after ${options.connectTimeoutMillis} ms")
        }
    } else {
        reactor.awaitConnect(fd)
    }
    // A failed non-blocking connect also reports "writable"; the outcome is in SO_ERROR.
    val err = socketError(fd)
    if (err == 0) return ReactorStream(fd, reactor)
    closeFd(fd)
    throw ConnectException("connect to $display failed: ${errnoMessage(err)} (errno $err)").also { it.code = err }
}
