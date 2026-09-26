@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.CompletableDeferred
import platform.windows.AI_NUMERICHOST
import platform.windows.AI_PASSIVE
import platform.windows.addrinfo
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import neton.io.win.neton_setsockopt_int
import platform.posix.AF_INET
import platform.posix.AF_INET6
import platform.posix.AF_UNSPEC
import platform.posix.INVALID_SOCKET
import platform.posix.IPPROTO_IPV6
import platform.posix.IPPROTO_TCP
import platform.windows.IPV6_V6ONLY
import platform.windows.WSAHOST_NOT_FOUND
import platform.posix.SOCKET_ERROR
import platform.posix.SOCK_STREAM
import platform.posix.TCP_NODELAY
import platform.posix.WSAEINPROGRESS
import platform.posix.WSAEWOULDBLOCK
import platform.posix.WSAGetLastError
import platform.posix.bind
import platform.posix.connect
import platform.posix.getsockname
import platform.posix.listen
import platform.posix.memset
import platform.posix.sockaddr
import platform.posix.socket
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.Worker

// Windows address resolution and TCP socket setup (SPEC §18.2, §20). Same contract as the POSIX
// implementation: getaddrinfo produces the sockaddr bytes, listen/connect are non-blocking.

private class Lookup(val addrs: List<SockAddr>, val error: Int)

private fun lookup(host: String, port: Int, passive: Boolean, numericOnly: Boolean): Lookup = memScoped {
    ensureWinsock()
    val hints = alloc<addrinfo>()
    memset(hints.ptr, 0, sizeOf<addrinfo>().convert())
    hints.ai_family = AF_UNSPEC
    hints.ai_socktype = SOCK_STREAM
    hints.ai_flags = (if (passive) AI_PASSIVE else 0) or (if (numericOnly) AI_NUMERICHOST else 0)
    val res = alloc<CPointerVar<addrinfo>>()
    val rc = getaddrinfo(host, port.toString(), hints.ptr, res.ptr)
    if (rc != 0) return@memScoped Lookup(emptyList(), rc)
    val out = ArrayList<SockAddr>()
    var p = res.value
    while (p != null) {
        val ai = p.pointed
        val sa = ai.ai_addr
        if (sa != null) out.add(SockAddr(ai.ai_family, ai.ai_family == AF_INET6, sa.reinterpret<ByteVar>().readBytes(ai.ai_addrlen.toInt())))
        p = ai.ai_next
    }
    freeaddrinfo(res.value)
    Lookup(out, 0)
}

@OptIn(ObsoleteWorkersApi::class)
private val resolverWorker: Worker by lazy { Worker.start(name = "neton-resolver") }

@OptIn(ObsoleteWorkersApi::class)
internal actual suspend fun resolve(host: String, port: Int, passive: Boolean): List<SockAddr> {
    require(port in 0..65535) { "port out of range: $port" }
    val numeric = lookup(host, port, passive, numericOnly = true)
    if (numeric.error == 0 && numeric.addrs.isNotEmpty()) return numeric.addrs
    // WSAHOST_NOT_FOUND is what getaddrinfo reports for a name when AI_NUMERICHOST forbids a lookup.
    if (numeric.error != 0 && numeric.error != WSAHOST_NOT_FOUND) throw ResolveException("cannot resolve '$host': error ${numeric.error}")
    val done = CompletableDeferred<Lookup>()
    resolverWorker.executeAfter(0L) { done.complete(lookup(host, port, passive, numericOnly = false)) }
    val r = done.await()
    if (r.error != 0 || r.addrs.isEmpty()) throw ResolveException("cannot resolve '$host': error ${r.error}")
    return r.addrs
}

/** 127.0.0.1 with port 0 (the wake pair's listener). */
internal fun loopbackAnyPort(): SockAddr = lookup("127.0.0.1", 0, passive = false, numericOnly = true).addrs.first()

/** The address [fd] is bound to (getsockname), e.g. to learn the port the OS picked. */
internal fun boundAddress(fd: Int): SockAddr = memScoped {
    val buf = allocArray<ByteVar>(128)
    val len = alloc<IntVar>(); len.value = 128
    check(getsockname(fd.toSocket(), buf.reinterpret(), len.ptr) == 0) { "getsockname failed (${WSAGetLastError()})" }
    val bytes = buf.readBytes(len.value)
    val family = (bytes[0].toInt() and 0xFF) or ((bytes[1].toInt() and 0xFF) shl 8)   // sockaddr.sa_family, little-endian
    SockAddr(family, family == AF_INET6, bytes)
}

internal actual fun tcpListenAddr(addr: SockAddr, display: String, options: SocketOptions): Int {
    ensureWinsock()
    val s = socket(addr.family, SOCK_STREAM, IPPROTO_TCP)
    check(s != INVALID_SOCKET) { "socket() failed (${WSAGetLastError()})" }
    val fd = s.toFd()
    applyListenerOptions(fd, options)   // no SO_REUSEADDR on Windows: it would let another socket take over a port in use
    if (addr.isIpv6) neton_setsockopt_int(s, IPPROTO_IPV6, IPV6_V6ONLY, 0)
    val rc = addr.bytes.usePinned { bind(s, it.addressOf(0).reinterpret<sockaddr>(), addr.bytes.size) }
    if (rc == SOCKET_ERROR) { val e = WSAGetLastError(); closeFd(fd); error("bind($display) failed: Winsock error $e") }
    if (listen(s, options.backlog) == SOCKET_ERROR) { val e = WSAGetLastError(); closeFd(fd); error("listen($display) failed: Winsock error $e") }
    setNonBlocking(fd)
    return fd
}

internal actual fun tcpConnectAddr(addr: SockAddr, display: String, options: SocketOptions): Int {
    ensureWinsock()
    val s = socket(addr.family, SOCK_STREAM, IPPROTO_TCP)
    check(s != INVALID_SOCKET) { "socket() failed (${WSAGetLastError()})" }
    val fd = s.toFd()
    setNonBlocking(fd)
    applyStreamOptions(fd, options)
    val rc = addr.bytes.usePinned { connect(s, it.addressOf(0).reinterpret<sockaddr>(), addr.bytes.size) }
    if (rc == SOCKET_ERROR) {
        val e = WSAGetLastError()
        if (e != WSAEWOULDBLOCK && e != WSAEINPROGRESS) {
            closeFd(fd)
            throw ConnectException("connect to $display failed: Winsock error $e")
        }
    }
    return fd
}

internal actual fun setNoDelay(fd: Int) {
    neton_setsockopt_int(fd.toSocket(), IPPROTO_TCP, TCP_NODELAY, 1)
}
