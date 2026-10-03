package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.Pinned

// The native readiness driver's data path: socket calls on pinned arrays, so a recv or send goes
// straight to the kernel with no copy (SPEC §17c). The JVM driver has its own (NioReactor).

/**
 * Non-blocking recv into [pinned] at [offset], at most [len] bytes. Returns the byte count (>0),
 * [EOF_RESULT] on a clean peer close, [WOULD_BLOCK] or [IO_ERROR] (see [lastSocketError]).
 * The caller commits the count into its buffer; nothing is allocated here (SPEC §17c).
 */
@OptIn(ExperimentalForeignApi::class)
internal expect fun recvPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int

/** Non-blocking send of [len] bytes from [pinned] at [offset]. Returns bytes sent, or [WOULD_BLOCK]/[IO_ERROR]. */
@OptIn(ExperimentalForeignApi::class)
internal expect fun sendPinned(fd: Int, pinned: Pinned<ByteArray>, offset: Int, len: Int): Int

/**
 * One vectored send of `bufs[from until from + count]` (SPEC §23.3): sendmsg / WSASend. Returns the
 * bytes sent (the caller advances the buffers), or [WOULD_BLOCK] / [IO_ERROR].
 */
@OptIn(ExperimentalForeignApi::class)
internal expect fun sendBuffers(fd: Int, bufs: Array<neton.io.bytes.Buffer>, from: Int, count: Int, pins: Array<Pinned<ByteArray>?>): Long
