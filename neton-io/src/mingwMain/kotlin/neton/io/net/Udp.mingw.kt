package neton.io.net

// SPEC §29: UDP on Windows (WSARecvMsg / WSASendMsg, USO / URO) is not implemented yet.
private fun unsupported(): Nothing = throw UnsupportedOperationException("UDP is not implemented on Windows yet (SPEC §29)")

internal actual fun udpBatchSize(): Int = 1
internal actual fun udpBind(family: Int, ip: ByteArray, port: Int, scope: Int, v6only: Boolean): Int = unsupported()
internal actual fun udpSetup(fd: Int, v6: Boolean, v6only: Boolean, caps: IntArray): Int = unsupported()
internal actual fun udpLocal(fd: Int, out: IntArray, ip: ByteArray): Int = unsupported()
internal actual fun udpBuffer(fd: Int, which: Int, value: Int): Int = unsupported()
internal actual fun udpRecv(fd: Int, batch: RecvBatch): Int = unsupported()
internal actual fun udpSend(fd: Int, sockV6: Boolean, t: Transmit, einvalMode: Boolean): Int = unsupported()
