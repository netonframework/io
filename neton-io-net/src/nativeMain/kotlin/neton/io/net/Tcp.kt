package neton.io.net

import neton.io.core.IoStream
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.coroutineContext

/** The [Reactor] driving the current coroutine (it is the dispatcher). */
internal suspend fun currentReactor(): Reactor = coroutineContext[ContinuationInterceptor] as Reactor

/** A listening TCP endpoint. */
internal class TcpServer(private val listenFd: Int, private val reactor: Reactor) {
    suspend fun accept(): IoStream = ReactorStream(reactor.accept(listenFd), reactor)
    fun close() = closeFd(listenFd)
}

/** Bind and listen on [host]:[port]. Must run inside a reactor. */
internal suspend fun listenTcpServer(host: String, port: Int): TcpServer =
    TcpServer(tcpListen(host, port), currentReactor())

/** Connect to [host]:[port], completing the non-blocking connect through the reactor. */
internal suspend fun connectStream(host: String, port: Int): IoStream {
    val reactor = currentReactor()
    val fd = tcpConnect(host, port)
    reactor.awaitConnect(fd)
    return ReactorStream(fd, reactor)
}
