@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.Pinned
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.set
import kotlinx.cinterop.value
import neton.io.win.neton_recv_nb
import neton.io.win.neton_send_nb
import neton.io.win.neton_getsockopt_int
import neton.io.win.neton_wsa_startup
import platform.posix.FIONBIO
import platform.posix.INVALID_SOCKET
import platform.posix.SOCKET
import platform.posix.SOL_SOCKET
import platform.posix.SO_ERROR
import platform.posix.WSAEINTR
import platform.posix.WSAEWOULDBLOCK
import platform.posix.WSAGetLastError
import platform.posix.accept
import platform.posix.closesocket
import platform.posix.ioctlsocket
import platform.posix.recv
import platform.posix.send

// Winsock implementation of the socket primitives (SPEC §20). A SOCKET is a handle (ULong); the
// reactor carries it as an Int — Windows socket handle values are small in practice, and
// [toFd] checks it. Readiness is reported by WSAPoll for now (correctness first); IOCP replaces
// it for performance.

/** Winsock is started once per process, on first use. */
private val winsockStarted: Boolean by lazy { neton_wsa_startup() == 0 }

internal fun ensureWinsock() = check(winsockStarted) { "WSAStartup failed" }

internal fun SOCKET.toFd(): Int {
    check(this != INVALID_SOCKET && this <= Int.MAX_VALUE.toULong()) { "socket handle $this does not fit the reactor's fd" }
    return this.toInt()
}

internal fun Int.toSocket(): SOCKET = this.toULong()

internal actual fun setNonBlocking(fd: Int) {
    memScoped {
        val mode = alloc<UIntVar>(); mode.value = 1u
        ioctlsocket(fd.toSocket(), FIONBIO.toInt(), mode.ptr)
    }
}

internal actual fun acceptOne(listenFd: Int): Int {
    val s = accept(listenFd.toSocket(), null, null)
    return if (s == INVALID_SOCKET) -1 else s.toFd()
}

internal actual fun closeFd(fd: Int) {
    closesocket(fd.toSocket())
}

internal actual fun socketError(fd: Int): Int = memScoped {
    val err = alloc<IntVar>()
    if (neton_getsockopt_int(fd.toSocket(), SOL_SOCKET, SO_ERROR, err.ptr) != 0) WSAGetLastError() else err.value
}

/**
 * The Winsock error of the last failed recv / send on this thread, as the call itself reported it (the neton_*_nb shims
 * read WSAGetLastError() inside the call). Read from Kotlin after the call returned, the thread's last error could
 * already be 0 ("read failed: Winsock error 0", CI: ParkCancellationTest, MultiReactorTest; SPEC §33.5).
 */
@kotlin.native.concurrent.ThreadLocal
private var savedSocketError = 0

/** A failed recv / send's result for its Winsock error [e]: WOULD_BLOCK, or IO_ERROR with [e] saved for [lastSocketError]. */
internal fun failedIo(e: Int): Int {
    if (e == WSAEWOULDBLOCK || e == WSAEINTR) return WOULD_BLOCK
    savedSocketError = e
    return IO_ERROR
}

internal actual fun lastSocketError(): Int {
    val e = savedSocketError
    savedSocketError = 0
    return if (e != 0) e else WSAGetLastError()
}

internal actual fun errnoMessage(code: Int): String = "Winsock error $code"

/** Windows has no SIGPIPE. */
internal actual fun suppressSigpipe(fd: Int): Boolean = true

internal actual fun recvPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = neton_recv_nb(fd.toSocket(), pinned.addressOf(offset), len)
    return when {
        n > 0 -> n
        n == 0 -> EOF_RESULT
        else -> failedIo(-n)
    }
}

/**
 * The most one non-blocking send() hands to Winsock. While the send buffer is not full Windows accepts a whole send
 * however large (buffering it in the kernel), so a 32 MiB write to a peer that does not read completed at once: no
 * backpressure, no write timeout, unbounded kernel memory per slow peer. Capping each call bounds that to one chunk
 * beyond the socket buffer; the next call then sees WSAEWOULDBLOCK and the writer parks, as on POSIX (SPEC §33).
 */
internal const val MAX_SEND_CHUNK = 256 * 1024

internal actual fun sendPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int {
    val n = neton_send_nb(fd.toSocket(), pinned.addressOf(offset), minOf(len, MAX_SEND_CHUNK))
    if (n > 0) return n
    return failedIo(-n)
}

/**
 * Windows has no pipe usable with WSAPoll, so the wake channel is a connected loopback TCP pair:
 * [read end (accepted side), write end (connecting side)], both non-blocking, Nagle off.
 */
internal actual fun createWakePipe(): IntArray {
    ensureWinsock()
    val addr = loopbackAnyPort()
    val listener = tcpListenAddr(addr, "127.0.0.1:0", SocketOptions(backlog = 1))
    try {
        val bound = boundAddress(listener)
        val writer = tcpConnectAddr(bound, "wake pipe")
        var reader = -1
        // The connect is non-blocking; on loopback the accept side is ready almost at once.
        for (i in 0 until 1000) {
            reader = acceptOne(listener)
            if (reader >= 0) break
            platform.posix.usleep(1000u)
        }
        check(reader >= 0) { "wake pipe: loopback accept failed (${lastSocketError()})" }
        setNonBlocking(reader)
        setNoDelay(writer)
        return intArrayOf(reader, writer)
    } finally {
        closeFd(listener)
    }
}

internal actual fun signalWakePipe(writeFd: Int): Unit = memScoped {
    val b = allocArray<kotlinx.cinterop.ByteVar>(1)
    b[0] = 1.toByte()
    send(writeFd.toSocket(), b, 1, 0)
    Unit
}

internal actual fun drainWakePipe(readFd: Int): Unit = memScoped {
    val buf = allocArray<kotlinx.cinterop.ByteVar>(64)
    while (recv(readFd.toSocket(), buf, 64, 0) > 0) { /* drain */ }
}
