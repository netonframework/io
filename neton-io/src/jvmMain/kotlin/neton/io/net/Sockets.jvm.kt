package neton.io.net

import java.io.IOException
import java.nio.channels.SelectableChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

// Socket primitives over NIO channels named by their [Channels] id. Data moves in [NioReactor],
// which keeps a reusable ByteBuffer view per buffer; these are the calls the shared layers make.

internal actual fun setNonBlocking(fd: Int) {
    (Channels[fd] as? SelectableChannel)?.configureBlocking(false)
}

internal actual fun acceptOne(listenFd: Int): Int {
    val server = Channels[listenFd] as? ServerSocketChannel ?: return -1
    return try {
        val client = server.accept() ?: return -1
        client.configureBlocking(false)
        Channels.add(client).also { if (Channels.isIpv6(listenFd)) Channels.markIpv6(it) }
    } catch (e: IOException) {
        JvmErrno.record(e)
        -1
    }
}

internal actual fun closeFd(fd: Int) {
    try { Channels.remove(fd)?.close() } catch (_: IOException) { }
}

/**
 * The outcome of a non-blocking connect. NIO completes it with finishConnect(), which also raises
 * the failure the kernel's SO_ERROR would report; the reactor calls this once the channel is
 * connectable, exactly where the POSIX path reads SO_ERROR.
 */
internal actual fun socketError(fd: Int): Int {
    val channel = Channels[fd] as? SocketChannel ?: return JvmErrno.EBADF
    if (!channel.isConnectionPending) return if (channel.isConnected) 0 else JvmErrno.ENOTCONN
    return try {
        if (channel.finishConnect()) 0 else JvmErrno.EINPROGRESS
    } catch (e: IOException) {
        JvmErrno.codeOf(e)
    }
}

internal actual fun lastSocketError(): Int = JvmErrno.lastError

internal actual fun errnoMessage(code: Int): String = JvmErrno.message(code)

/** The JVM ignores SIGPIPE; a write to a gone peer fails with an IOException. */
internal actual fun suppressSigpipe(fd: Int): Boolean = true

internal actual fun shutdownWrite(fd: Int) {
    try { (Channels[fd] as? SocketChannel)?.socket()?.shutdownOutput() } catch (_: IOException) { }
}

// NioReactor wakes its Selector directly (as the IOCP driver posts to its port), so the shared
// reactor's self-pipe is never created on the JVM.
internal actual fun createWakePipe(): IntArray =
    throw UnsupportedOperationException("the NIO reactor wakes its Selector, not a pipe")

internal actual fun signalWakePipe(writeFd: Int) = Unit

internal actual fun drainWakePipe(readFd: Int) = Unit
