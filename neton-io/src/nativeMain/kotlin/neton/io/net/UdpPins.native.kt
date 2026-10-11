@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.Pinned
import kotlinx.cinterop.pin

internal actual class RecvBatchPins actual constructor(batch: RecvBatch) {
    val buffer = batch.buffer.pin(); val lens = batch.lens.pin(); val strides = batch.strides.pin()
    val families = batch.families.pin(); val ips = batch.ips.pin(); val ports = batch.ports.pin()
    val scopes = batch.scopes.pin(); val ecns = batch.ecns.pin(); val dstFamilies = batch.dstFamilies.pin()
    val dstIps = batch.dstIps.pin()

    actual fun unpin() {
        listOf<Pinned<*>>(buffer, lens, strides, families, ips, ports, scopes, ecns, dstFamilies, dstIps).forEach { it.unpin() }
    }
}

internal actual class TransmitPins actual constructor(transmit: Transmit) {
    val buffer = transmit.buffer.pin(); val dstIp = transmit.dstIp.pin(); val srcIp = transmit.srcIp.pin()

    actual fun unpin() { buffer.unpin(); dstIp.unpin(); srcIp.unpin() }
}

// The names the platform shims (posixMain, mingwMain) use.
internal val RecvBatch.pBuffer: Pinned<ByteArray> get() = pins.buffer
internal val RecvBatch.pLens: Pinned<IntArray> get() = pins.lens
internal val RecvBatch.pStrides: Pinned<IntArray> get() = pins.strides
internal val RecvBatch.pFamilies: Pinned<IntArray> get() = pins.families
internal val RecvBatch.pIps: Pinned<ByteArray> get() = pins.ips
internal val RecvBatch.pPorts: Pinned<IntArray> get() = pins.ports
internal val RecvBatch.pScopes: Pinned<IntArray> get() = pins.scopes
internal val RecvBatch.pEcns: Pinned<IntArray> get() = pins.ecns
internal val RecvBatch.pDstFamilies: Pinned<IntArray> get() = pins.dstFamilies
internal val RecvBatch.pDstIps: Pinned<ByteArray> get() = pins.dstIps
internal val Transmit.pBuffer: Pinned<ByteArray> get() = pins.buffer
internal val Transmit.pDstIp: Pinned<ByteArray> get() = pins.dstIp
internal val Transmit.pSrcIp: Pinned<ByteArray> get() = pins.srcIp
