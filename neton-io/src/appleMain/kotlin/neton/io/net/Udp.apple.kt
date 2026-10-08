@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString

/**
 * SPEC §29.9 ⚖️: up to 32 datagrams per receive call, one recvmsg each inside the shim until the socket is empty
 * (quinn-udp: one per call without its `fast-apple-datapath`). `NETON_IO_UDP_BATCH` overrides it (1 to 32), for
 * measurement.
 */
internal actual val udpPlatformBatch: Int =
    platform.posix.getenv("NETON_IO_UDP_BATCH")?.toKString()?.toIntOrNull()?.coerceIn(1, 32) ?: 32
