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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext
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

private class Lookup(val addrs: List<SockAddr>, val gaiError: Int, val message: String?)

/** Blocking `getaddrinfo`. Never call it on a reactor thread without [numericOnly]. */
@OptIn(ExperimentalForeignApi::class)
private fun lookup(host: String, port: Int, passive: Boolean, numericOnly: Boolean): Lookup = memScoped {
    val hints = alloc<addrinfo>()
    neton.io.posixshim.neton_zero(hints.ptr, sizeOf<addrinfo>().toInt())
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
        if (sa != null) out.add(SockAddr(ai.ai_family, ai.ai_family == AF_INET6, sa.reinterpret<ByteVar>().readBytes(neton.io.posixshim.neton_ai_addrlen(p))))
        p = ai.ai_next
    }
    freeaddrinfo(res.value)
    Lookup(out, 0, null)
}

internal actual suspend fun resolve(host: String, port: Int, passive: Boolean): List<SockAddr> {
    require(port in 0..65535) { "port out of range: $port" }
    val numeric = lookup(host, port, passive, numericOnly = true)
    if (numeric.gaiError == 0 && numeric.addrs.isNotEmpty()) return numeric.addrs
    if (numeric.gaiError != 0 && numeric.gaiError != EAI_NONAME) {
        throw ResolveException("cannot resolve '$host': ${numeric.message}")
    }
    // SPEC §27.5: `getaddrinfo` blocks, so it runs on kotlinx's IO pool (in parallel with other
    // lookups, never on a reactor thread); the caller resumes on its own reactor.
    val r = withContext(Dispatchers.IO) { lookup(host, port, passive, numericOnly = false) }
    if (r.gaiError != 0 || r.addrs.isEmpty()) throw ResolveException("cannot resolve '$host': ${r.message ?: "no addresses"}")
    return r.addrs
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun tcpListenAddr(addr: SockAddr, display: String, options: SocketOptions): Int = memScoped {
    val fd = socket(addr.family, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    applyListenerOptions(fd, options)
    if (addr.isIpv6) {
        val zero = alloc<IntVar>(); zero.value = 0
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, zero.ptr, sizeOf<IntVar>().convert())
    }
    val rc = addr.bytes.usePinned { bind(fd, it.addressOf(0).reinterpret<sockaddr>(), addr.bytes.size.convert()) }
    if (rc != 0) {
        val e = errno; closeFd(fd)
        error("bind($display) failed: ${strerror(e)?.toKString()} (errno=$e)")
    }
    if (listen(fd, options.backlog) != 0) {
        val e = errno; closeFd(fd)
        error("listen($display) failed: ${strerror(e)?.toKString()} (errno=$e)")
    }
    setNonBlocking(fd)
    fd
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun tcpConnectAddr(addr: SockAddr, display: String, options: SocketOptions): Int {
    val fd = socket(addr.family, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed: ${strerror(errno)?.toKString()} (errno=$errno)" }
    setNonBlocking(fd)
    suppressSigpipe(fd)
    applyStreamOptions(fd, options)          // before connect: buffer sizes fix the window scale
    val rc = addr.bytes.usePinned { connect(fd, it.addressOf(0).reinterpret<sockaddr>(), addr.bytes.size.convert()) }
    if (rc != 0 && errno != EINPROGRESS) {
        val err = errno
        closeFd(fd)
        throw ConnectException("connect to $display failed: ${errnoMessage(err)} (errno $err)").also { it.code = err }
    }
    return fd
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun setNoDelay(fd: Int): Unit = memScoped {
    val one = alloc<IntVar>(); one.value = 1
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, one.ptr, sizeOf<IntVar>().convert())
}

