package neton.io.net

import neton.io.core.IoStream
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.coroutineContext

/** Retrieve the [EventLoop] driving the current coroutine. */
internal suspend fun currentEventLoop(): EventLoop =
    coroutineContext[ContinuationInterceptor] as EventLoop

/** A listening TCP endpoint. */
internal class TcpServer(private val listenFd: Int, private val loop: EventLoop) {

    /** Accept the next connection, parking on the reactor while none is pending. */
    suspend fun accept(): IoStream {
        while (true) {
            val clientFd = acceptOne(listenFd)
            if (clientFd >= 0) {
                setNonBlocking(clientFd)
                return SocketStream(clientFd, loop)
            }
            loop.waitReadable(listenFd)
        }
    }

    fun close() = closeFd(listenFd)
}

/** Bind and listen on [host]:[port]. Must run inside [EventLoop.run]. */
internal suspend fun listenTcp(host: String, port: Int): TcpServer {
    val loop = currentEventLoop()
    val fd = tcpListen(host, port)
    return TcpServer(fd, loop)
}

/** Connect to [host]:[port], completing the non-blocking connect via the reactor. */
internal suspend fun connectTcp(host: String, port: Int): IoStream {
    val loop = currentEventLoop()
    val fd = tcpConnect(host, port)
    // Non-blocking connect signals completion by becoming writable.
    loop.waitWritable(fd)
    return SocketStream(fd, loop)
}
