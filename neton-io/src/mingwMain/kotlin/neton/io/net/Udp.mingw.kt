@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import neton.io.win.neton_wudp_bind
import neton.io.win.neton_wudp_buffer
import neton.io.win.neton_wudp_local
import neton.io.win.neton_wudp_recv
import neton.io.win.neton_wudp_send
import neton.io.win.neton_wudp_setup

// SPEC §29.7: UDP on Windows after quinn-udp 0.11 windows.rs (WSARecvMsg / WSASendMsg with PKTINFO and ECN, USO when
// the stack accepts it), in winshim.def. One datagram per receive call, as in the reference (BATCH_SIZE 1).

internal actual fun udpBatchSize(): Int = 1

internal actual fun udpBind(family: Int, ip: ByteArray, port: Int, scope: Int, v6only: Boolean): Int {
    ensureWinsock()
    return ip.usePinned { neton_wudp_bind(family, it.addressOf(0).reinterpret(), port, scope.toUInt(), if (v6only) 1 else 0) }
}

internal actual fun udpSetup(fd: Int, v6: Boolean, v6only: Boolean, caps: IntArray): Int = caps.usePinned {
    neton_wudp_setup(fd.toSocket(), if (v6) 1 else 0, if (v6only) 1 else 0, it.addressOf(0), it.addressOf(1))
}

internal actual fun udpLocal(fd: Int, out: IntArray, ip: ByteArray): Int = out.usePinned { o -> ip.usePinned { p ->
    neton_wudp_local(fd.toSocket(), o.addressOf(0), p.addressOf(0).reinterpret(), o.addressOf(1), o.addressOf(2).reinterpret<UIntVar>())
} }

internal actual fun udpBuffer(fd: Int, which: Int, value: Int): Int = neton_wudp_buffer(fd.toSocket(), which, value)

private const val WSAEINVAL_CODE = 10022
private const val WSAEWOULDBLOCK_CODE = 10035
private const val WSAEMSGSIZE_CODE = 10040
private const val WSAENETRESET_CODE = 10052
private const val WSAECONNRESET_CODE = 10054

/** A Winsock error as the shared UDP_* codes. A port-unreachable ICMP shows as WSAECONNRESET on the next receive. */
private fun code(negError: Int): Int = when (val e = -negError) {
    WSAEWOULDBLOCK_CODE -> UDP_WOULD_BLOCK
    WSAEMSGSIZE_CODE -> UDP_MSG_SIZE
    WSAEINVAL_CODE -> UDP_EINVAL
    WSAECONNRESET_CODE, WSAENETRESET_CODE -> UDP_CONN_ERROR
    else -> -(1000 + e)
}

internal actual fun udpRecv(fd: Int, batch: RecvBatch): Int {
    val n = neton_wudp_recv(
        fd.toSocket(), batch.pBuffer.addressOf(0).reinterpret<UByteVar>(), batch.slotSize,
        batch.pLens.addressOf(0), batch.pStrides.addressOf(0), batch.pFamilies.addressOf(0),
        batch.pIps.addressOf(0).reinterpret(), batch.pPorts.addressOf(0), batch.pScopes.addressOf(0).reinterpret<UIntVar>(),
        batch.pEcns.addressOf(0), batch.pDstFamilies.addressOf(0), batch.pDstIps.addressOf(0).reinterpret(),
    )
    return if (n >= 0) n else code(n)
}

internal actual fun udpSend(fd: Int, sockV6: Boolean, t: Transmit, einvalMode: Boolean): Int {
    val r = neton_wudp_send(
        fd.toSocket(), if (sockV6) 1 else 0, t.pBuffer.addressOf(0).reinterpret(), t.length,
        t.dstFamily, t.pDstIp.addressOf(0).reinterpret(), t.dstPort, t.dstScope.toUInt(),
        t.ecn?.bits ?: 0, t.segmentSize, t.srcFamily, t.pSrcIp.addressOf(0).reinterpret(), if (einvalMode) 1 else 0,
    )
    return if (r >= 0) r else code(r)
}
