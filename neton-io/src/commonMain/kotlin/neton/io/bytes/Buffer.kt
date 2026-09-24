package neton.io.bytes

/**
 * A growable byte buffer with separate reader and writer cursors.
 *
 * The current backing is a managed [ByteArray]; the type is designed so the backing can
 * later move to native/pinned memory (for zero-copy syscalls and completion-based drivers)
 * without changing this API.
 *
 * Semantics mirror a minimal `BytesMut`: append on write, consume on read, and reclaim the
 * already-read prefix so a long-lived buffer does not grow without bound.
 */
class Buffer(initialCapacity: Int = DEFAULT_CAPACITY) {

    private var array = ByteArray(initialCapacity.coerceAtLeast(16))
    private var readerIndex = 0
    private var writerIndex = 0

    /** Number of bytes available to read. */
    val readableBytes: Int get() = writerIndex - readerIndex

    val isEmpty: Boolean get() = readableBytes == 0

    // ---- write ----

    fun writeByte(b: Byte) {
        ensureWritable(1)
        array[writerIndex++] = b
    }

    fun writeBytes(src: ByteArray, offset: Int = 0, length: Int = src.size - offset) {
        require(offset >= 0 && length >= 0 && offset + length <= src.size) { "bad range" }
        if (length == 0) return
        ensureWritable(length)
        src.copyInto(array, writerIndex, offset, offset + length)
        writerIndex += length
    }

    // ---- read ----

    /** Read and consume [length] bytes, returning a fresh array. */
    fun readBytes(length: Int): ByteArray {
        require(length in 0..readableBytes) { "length=$length readable=$readableBytes" }
        val out = array.copyOfRange(readerIndex, readerIndex + length)
        readerIndex += length
        resetIfDrained()
        return out
    }

    /** Consume all readable bytes. */
    fun readAll(): ByteArray = readBytes(readableBytes)

    /** Skip [n] readable bytes. */
    fun skip(n: Int) {
        require(n in 0..readableBytes)
        readerIndex += n
        resetIfDrained()
    }

    /** Readable byte at offset [i] from the reader cursor, without consuming. */
    fun getByte(i: Int): Byte {
        require(i in 0 until readableBytes)
        return array[readerIndex + i]
    }

    /** Index of byte [b] within the readable region relative to the reader cursor, or -1. */
    fun indexOf(b: Byte): Int {
        for (i in readerIndex until writerIndex) if (array[i] == b) return i - readerIndex
        return -1
    }

    /** Snapshot of the readable region without consuming (test/debug aid). */
    fun peekAll(): ByteArray = array.copyOfRange(readerIndex, writerIndex)

    fun clear() {
        readerIndex = 0
        writerIndex = 0
    }

    /** Move unread data to the front of the array, reclaiming already-read space. */
    fun discardReadBytes() {
        if (readerIndex == 0) return
        array.copyInto(array, 0, readerIndex, writerIndex)
        writerIndex -= readerIndex
        readerIndex = 0
    }

    // ---- direct I/O (fill/drain the backing memory in place, no intermediate copy) ----

    /** Ensure at least [min] writable bytes and return the writable capacity now available. */
    fun reserve(min: Int): Int {
        ensureWritable(min)
        return array.size - writerIndex
    }

    /** Backing array, valid until the next write/reserve. Use with [writerIndex]/[readerIndex]. */
    fun backingArray(): ByteArray = array

    /** Current writer position into [backingArray]. */
    fun writerIndex(): Int = writerIndex

    /** Current reader position into [backingArray]. */
    fun readerIndex(): Int = readerIndex

    /** Commit [n] bytes filled directly into the backing array at the writer position. */
    fun commitWrite(n: Int) {
        require(n >= 0 && writerIndex + n <= array.size)
        writerIndex += n
    }

    /** Consume [n] readable bytes drained directly from the backing array. */
    fun consume(n: Int) {
        require(n in 0..readableBytes)
        readerIndex += n
        resetIfDrained()
    }

    private fun resetIfDrained() {
        if (readerIndex == writerIndex) clear()
    }

    private fun ensureWritable(length: Int) {
        if (writerIndex + length <= array.size) return
        if (readerIndex > 0) {
            discardReadBytes()
            if (writerIndex + length <= array.size) return
        }
        var newCapacity = array.size * 2
        while (newCapacity < writerIndex + length) newCapacity *= 2
        array = array.copyOf(newCapacity)
    }

    companion object {
        const val DEFAULT_CAPACITY = 1024
    }
}
