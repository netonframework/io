package neton.io.net

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.toKString
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.CompletableDeferred
import platform.posix.AF_INET6
import platform.posix.AF_UNSPEC
import platform.posix.AI_NUMERICHOST
import platform.posix.AI_PASSIVE
import platform.posix.EAI_NONAME
import platform.posix.EINPROGRESS
import platform.posix.IPPROTO_IPV6
import platform.posix.IPPROTO_TCP
import platform.posix.IPV6_V6ONLY
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_REUSEADDR
import platform.posix.TCP_NODELAY
import platform.posix.addrinfo
import platform.posix.bind
import platform.posix.connect
import platform.posix.errno
import platform.posix.freeaddrinfo
import platform.posix.gai_strerror
import platform.posix.getaddrinfo
import platform.posix.listen
import platform.posix.memset
import platform.posix.setsockopt
import platform.posix.sockaddr
import platform.posix.socket
import platform.posix.strerror
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.Worker

/**
 * One resolved socket address: the address family and the raw `sockaddr` bytes exactly as
 * `getaddrinfo` produced them (SPEC §18.2). Keeping the bytes opaque is what lets IPv4 and IPv6,
 * Linux and Apple (whose `sockaddr_in*` carry an extra length byte) share one code path.
 */
internal class SockAddr(val family: Int, val bytes: ByteArray) {
    val isIpv6: Boolean get() = family == AF_INET6
}

/** A host that could not be resolved (or an address that is not valid for the requested use). */
internal class ResolveException(message: String) : Exception(message)

private class Lookup(val addrs: List<SockAddr>, val gaiError: Int, val message: String?)

/** Blocking `getaddrinfo`. Never call it on a reactor thread without [numericOnly]. */
@OptIn(ExperimentalForeignApi::class)
private fun lookup(host: String, port: Int, passive: Boolean, numericOnly: Boolean): Lookup = memScoped {
    val hints = alloc<addrinfo>()
    memset(hints.ptr, 0, sizeOf<addrinfo>().convert())
    hints.ai_family = AF_UNSPEC
    hints.ai_socktype = SOCK_STREAM
    hints.ai_flags = (if (passive) AI_PASSIVE else 0) or (if (numericOnly) AI_NUMERICHOST else 0)
    val res = alloc<CPointerVar<addrinfo>>()
    val rc = getaddrinfo(host, port.toString(), hints.ptr, res.ptr)
    if (rc != 0) return@memScoped Lookup(emptyList(), rc, gai_strerror(rc)?.toKString())
    val out = ArrayList<SockAddr>()
    var p = res.value
    while (p != null) {
        val ai = p.pointed
        val sa = ai.ai_addr
        if (sa != null) out.add(SockAddr(ai.ai_family, sa.reinterpret<ByteVar>().readBytes(ai.ai_addrlen.toInt())))
        p = ai.ai_next
    }
    freeaddrinfo(res.value)
    Lookup(out, 0, null)
}

/** One thread for name lookups: `getaddrinfo` blocks, so it never runs on a reactor thread. */
@OptIn(ObsoleteWorkersApi::class)
private val resolverWorker: Worker by lazy { Worker.start(name = "neton-resolver") }

/**
 * Resolve [host]:[port] to socket addresses, in the order `getaddrinfo` returns them (RFC 6724).
 * Literals (`127.0.0.1`, `::1`, `0.0.0.0`, `::`) resolve inline with `AI_NUMERICHOST`, which never
 * blocks; names are looked up on the resolver thread and the result is delivered back to the
 * calling reactor through its dispatcher.
 */
@OptIn(ObsoleteWorkersApi::class)
internal suspend fun resolve(host: String, port: Int, passive: Boolean): List<SockAddr> {
    require(port in 0..65535) { "port out of range: $port" }
    val numeric = lookup(host, port, passive, numericOnly = true)
    if (numeric.gaiError == 0 && numeric.addrs.isNotEmpty()) return numeric.addrs
    if (numeric.gaiError != 0 && numeric.gaiError != EAI_NONAME) {
        throw ResolveException("cannot resolve '$host': ${numeric.message}")
    }
    val done = CompletableDeferred<Lookup>()
    resolverWorker.executeAfter(0L) { done.complete(lookup(host, port, passive, numericOnly = false)) }
    val r = done.await()
    if (r.gaiError != 0 || r.addrs.isEmpty()) throw ResolveException("cannot resolve '$host': ${r.message ?: "no addresses"}")
    return r.addrs
}

/** Open, bind and listen on [addr]; non-blocking. `::` listens dual-stack (IPv4 as mapped addresses). */
@OptIn(ExperimentalForeignApi::class)
internal fun tcpListenAddr(addr: SockAddr, display: String, backlog: Int = 1024): Int = memScoped {
    val fd = socket(addr.family, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    val one = alloc<IntVar>(); one.value = 1
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, one.ptr, sizeOf<IntVar>().convert())
    if (addr.isIpv6) {
        val zero = alloc<IntVar>(); zero.value = 0
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, zero.ptr, sizeOf<IntVar>().convert())
    }
    val rc = addr.bytes.usePinned { bind(fd, it.addressOf(0).reinterpret<sockaddr>(), addr.bytes.size.convert()) }
    if (rc != 0) {
        val e = errno; closeFd(fd)
        error("bind($display) failed: ${strerror(e)?.toKString()} (errno=$e)")
    }
    if (listen(fd, backlog) != 0) {
        val e = errno; closeFd(fd)
        error("listen($display) failed: ${strerror(e)?.toKString()} (errno=$e)")
    }
    setNonBlocking(fd)
    fd
}

/**
 * Open a socket for [addr] and start a non-blocking connect; completion is observed as
 * writability by the reactor. Returns the fd, or throws [ConnectException] if the connect failed
 * immediately.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun tcpConnectAddr(addr: SockAddr, display: String): Int {
    val fd = socket(addr.family, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    setNonBlocking(fd)
    suppressSigpipe(fd)
    setNoDelay(fd)
    val rc = addr.bytes.usePinned { connect(fd, it.addressOf(0).reinterpret<sockaddr>(), addr.bytes.size.convert()) }
    if (rc != 0 && errno != EINPROGRESS) {
        val err = errno
        closeFd(fd)
        throw ConnectException("connect to $display failed: ${errnoMessage(err)} (errno $err)")
    }
    return fd
}

/**
 * Disable Nagle on a TCP stream (SPEC §19.6). A response written in several pieces otherwise has
 * each piece's partial tail held until the peer ACKs, and a peer delaying its ACK stalls the
 * exchange for ~40 ms. geario sets it on every stream too.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun setNoDelay(fd: Int): Unit = memScoped {
    val one = alloc<IntVar>(); one.value = 1
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, one.ptr, sizeOf<IntVar>().convert())
}

/** `host:port`, with IPv6 literals bracketed. */
internal fun hostPort(host: String, port: Int): String = if (':' in host) "[$host]:$port" else "$host:$port"
