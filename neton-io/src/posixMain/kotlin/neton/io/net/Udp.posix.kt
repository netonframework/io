@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import neton.io.posixshim.neton_udp_bind
import neton.io.posixshim.neton_udp_buffer
import neton.io.posixshim.neton_udp_local
import neton.io.posixshim.neton_udp_recv
import neton.io.posixshim.neton_udp_send
import neton.io.posixshim.neton_udp_setup
import platform.posix.EAGAIN
import platform.posix.ECONNREFUSED
import platform.posix.ECONNRESET
import platform.posix.EINVAL
import platform.posix.EIO
import platform.posix.EMSGSIZE
import platform.posix.EWOULDBLOCK

internal actual fun udpBatchSize(): Int = udpPlatformBatch

/** recvmmsg batch on Linux / Android, 1 on Apple (quinn-udp BATCH_SIZE). */
internal expect val udpPlatformBatch: Int

internal actual fun udpBind(family: Int, ip: ByteArray, port: Int, scope: Int, v6only: Boolean): Int =
    ip.usePinned { neton_udp_bind(family, it.addressOf(0).reinterpret(), port, scope.toUInt(), if (v6only) 1 else 0) }

internal actual fun udpSetup(fd: Int, v6: Boolean, v6only: Boolean, caps: IntArray): Int = caps.usePinned {
    neton_udp_setup(fd, if (v6) 1 else 0, if (v6only) 1 else 0, it.addressOf(0), it.addressOf(1))
}

internal actual fun udpLocal(fd: Int, out: IntArray, ip: ByteArray): Int = out.usePinned { o -> ip.usePinned { p ->
    neton_udp_local(fd, o.addressOf(0), p.addressOf(0).reinterpret(), o.addressOf(1), o.addressOf(2).reinterpret<UIntVar>())
} }

internal actual fun udpBuffer(fd: Int, which: Int, value: Int): Int = neton_udp_buffer(fd, which, value)

private fun code(negErrno: Int): Int {
    val e = -negErrno
    return when (e) {
        EAGAIN, EWOULDBLOCK -> UDP_WOULD_BLOCK
        EMSGSIZE -> UDP_MSG_SIZE
        EIO -> UDP_EIO
        EINVAL -> UDP_EINVAL
        ECONNREFUSED, ECONNRESET -> UDP_CONN_ERROR
        else -> -(1000 + e)
    }
}

internal actual fun udpRecv(fd: Int, batch: RecvBatch): Int {
    val n = neton_udp_recv(
        fd, batch.pBuffer.addressOf(0).reinterpret<UByteVar>(), batch.slotSize, batch.capacity,
        batch.pLens.addressOf(0), batch.pStrides.addressOf(0), batch.pFamilies.addressOf(0),
        batch.pIps.addressOf(0).reinterpret(), batch.pPorts.addressOf(0), batch.pScopes.addressOf(0).reinterpret<UIntVar>(),
        batch.pEcns.addressOf(0), batch.pDstFamilies.addressOf(0), batch.pDstIps.addressOf(0).reinterpret(),
    )
    return if (n >= 0) n else code(n)
}

internal actual fun udpSend(fd: Int, sockV6: Boolean, t: Transmit, einvalMode: Boolean): Int {
    val r = neton_udp_send(
        fd, if (sockV6) 1 else 0, t.pBuffer.addressOf(0).reinterpret(), t.length,
        t.dstFamily, t.pDstIp.addressOf(0).reinterpret(), t.dstPort, t.dstScope.toUInt(),
        t.ecn?.bits ?: 0, t.segmentSize, t.srcFamily, t.pSrcIp.addressOf(0).reinterpret(), if (einvalMode) 1 else 0,
    )
    return if (r >= 0) r else code(r)
}
