package neton.io.net

import java.io.IOException
import java.net.BindException
import java.net.NoRouteToHostException
import java.net.PortUnreachableException
import java.net.SocketTimeoutException
import java.nio.channels.Channel
import java.nio.channels.ClosedChannelException
import java.nio.channels.UnresolvedAddressException
import java.util.concurrent.atomic.AtomicReferenceArray

/**
 * The JVM's file descriptor table. The shared layers name a socket by a small Int, as the native
 * drivers do with fds (and the Windows driver with its own SOCKET table); here the Int indexes NIO
 * channels. Ids are reused lowest-first like kernel fds, so the reactor's per-fd arrays stay small.
 *
 * A socket is created on one reactor thread and may be handed to another ([ReactorGroup]'s accept
 * hand-off), so the table is shared: writes take a lock, reads are a volatile load and an atomic
 * array read.
 */
internal object Channels {
    private val lock = Any()
    @Volatile private var table = AtomicReferenceArray<Channel?>(64)
    private val free = java.util.TreeSet<Int>()
    private var next = 0
    /**
     * Ids of IPv6-family sockets. The JDK reports an IPv4 peer of a dual-stack IPv6 socket as plain
     * IPv4; native sockets report it as `::ffff:a.b.c.d`, and the address API promises the native
     * form, so the family is remembered here (set at bind/connect, inherited on accept).
     */
    private val ipv6 = HashSet<Int>()

    fun markIpv6(id: Int) = synchronized(lock) { ipv6.add(id) }

    fun isIpv6(id: Int): Boolean = synchronized(lock) { id in ipv6 }

    fun add(channel: Channel): Int = synchronized(lock) {
        val id = free.pollFirst() ?: next++
        var t = table
        if (id >= t.length()) {
            var n = t.length()
            while (n <= id) n *= 2
            val grown = AtomicReferenceArray<Channel?>(n)
            for (i in 0 until t.length()) grown.set(i, t.get(i))
            t = grown
            table = grown
        }
        t.set(id, channel)
        id
    }

    operator fun get(id: Int): Channel? {
        val t = table
        return if (id in 0 until t.length()) t.get(id) else null
    }

    fun remove(id: Int): Channel? = synchronized(lock) {
        val t = table
        if (id !in 0 until t.length()) return null
        val channel = t.getAndSet(id, null) ?: return null
        ipv6.remove(id)
        free.add(id)
        channel
    }
}

/**
 * errno for the JVM. NIO reports failures as exceptions; the shared layers expect a code (they put
 * it in [neton.io.core.IoException.errno] and compare against ECONNRESET). The Linux
 * values are used so a code reads the same in a log from either kind of target.
 */
internal object JvmErrno {
    const val EIO = 5
    const val EBADF = 9
    const val EPIPE = 32
    const val EADDRINUSE = 98
    const val ENETUNREACH = 101
    const val ECONNRESET = 104
    const val ENOTCONN = 107
    const val ETIMEDOUT = 110
    const val ECONNREFUSED = 111
    const val EHOSTUNREACH = 113
    const val EINPROGRESS = 115

    private val last = object : ThreadLocal<IntArray>() {
        override fun initialValue() = IntArray(1)
    }

    var lastError: Int
        get() = last.get()[0]
        set(value) { last.get()[0] = value }

    /** Record [t] as this thread's last socket error and return its code. */
    fun record(t: Throwable): Int = codeOf(t).also { lastError = it }

    fun codeOf(t: Throwable): Int {
        val message = t.message.orEmpty().lowercase()
        return when {
            t is ClosedChannelException -> EBADF
            t is BindException -> EADDRINUSE
            t is NoRouteToHostException -> EHOSTUNREACH
            t is PortUnreachableException -> ECONNREFUSED
            t is SocketTimeoutException -> ETIMEDOUT
            t is UnresolvedAddressException -> EHOSTUNREACH
            "refused" in message -> ECONNREFUSED
            "reset" in message -> ECONNRESET
            "broken pipe" in message -> EPIPE
            "timed out" in message -> ETIMEDOUT
            "network is unreachable" in message -> ENETUNREACH
            "no route" in message || "host is unreachable" in message -> EHOSTUNREACH
            t is IOException -> EIO
            else -> EIO
        }
    }

    fun message(code: Int): String = when (code) {
        EIO -> "Input/output error"
        EBADF -> "Bad file descriptor"
        EPIPE -> "Broken pipe"
        EADDRINUSE -> "Address already in use"
        ENETUNREACH -> "Network is unreachable"
        ECONNRESET -> "Connection reset by peer"
        ENOTCONN -> "Transport endpoint is not connected"
        ETIMEDOUT -> "Connection timed out"
        ECONNREFUSED -> "Connection refused"
        EHOSTUNREACH -> "No route to host"
        EINPROGRESS -> "Operation now in progress"
        else -> "errno $code"
    }
}
