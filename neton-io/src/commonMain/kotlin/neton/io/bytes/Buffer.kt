package neton.io.bytes

/**
 * A growable byte buffer with separate reader and writer cursors.
 *
 * Semantics mirror a minimal `BytesMut`: append on write, consume on read, and reclaim the
 * already-read prefix so a long-lived buffer does not grow without bound.
 *
 * Pooled buffers (SPEC §23.7, `Buffer(pooled = true)`) take their array from the thread's
 * [BufferPool] on first write and give it back as soon as reading drains them, so an idle
 * connection holds no buffer memory. [clear] keeps the array (a driver may still have it pinned).
 *
 * [readSlice] returns a zero-copy [Bytes]. The array it points into is then *shared*: the buffer
 * never writes into already-read space of that array again (no compaction, no reset to 0); when it
 * would, it moves to a fresh array instead, and the shared one is left to the slices and the GC —
 * never returned to the pool.
 */
class Buffer private constructor(
    private var array: ByteArray,
    /** Whether arrays come from, and go back to, the thread's [BufferPool]. */
    val pooled: Boolean,
    /** Capacity of the first array taken lazily (pooled, or after leaving a shared array). */
    private val firstCapacity: Int,
) {
    constructor(initialCapacity: Int = DEFAULT_CAPACITY, pooled: Boolean = false) : this(
        if (pooled) EMPTY_ARRAY else ByteArray(initialCapacity.coerceAtLeast(16)),
        pooled,
        initialCapacity.coerceAtLeast(16),
    )

    private var readerIndex = 0
    private var writerIndex = 0
    /** Slices point into [array]: already-read space must not be rewritten. */
    private var shared = false
    /** [array] belongs to someone else (a wrapped [Bytes]): nothing may be written into it. */
    private var borrowed = false

    /** Number of bytes available to read. */
    val readableBytes: Int get() = writerIndex - readerIndex

    val isEmpty: Boolean get() = readableBytes == 0

    /** Size of the array currently held (0 for a pooled buffer that holds none). */
    val capacity: Int get() = array.size

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

    /** Append a copy of [src]. */
    fun writeBytes(src: Bytes) {
        if (src.size == 0) return
        ensureWritable(src.size)
        src.copyInto(array, writerIndex)
        writerIndex += src.size
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

    /** Read and consume [length] bytes as a zero-copy slice (SPEC §23.7). */
    fun readSlice(length: Int): Bytes {
        require(length in 0..readableBytes) { "length=$length readable=$readableBytes" }
        if (length == 0) return Bytes.EMPTY
        val out = Bytes(array, readerIndex, length)
        shared = true
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

    /** Drop all readable bytes. The array is kept (pooled or not) unless slices share it. */
    fun clear() {
        readerIndex = 0
        writerIndex = 0
        if (shared || borrowed) leaveArray()
    }

    /** Move unread data to the front, reclaiming already-read space (into a fresh array if shared). */
    fun discardReadBytes() {
        if (readerIndex == 0) return
        if (shared || borrowed) { moveTo(allocate(maxOf(array.size, readableBytes))); return }
        array.copyInto(array, 0, readerIndex, writerIndex)
        writerIndex -= readerIndex
        readerIndex = 0
    }

    /**
     * Give a pooled buffer's array back if nothing is left to read, so a connection parked in a
     * read holds no memory. Drivers call this before parking; a no-op for unpooled buffers.
     */
    fun releaseIfIdle() {
        if (!pooled || readerIndex != writerIndex || array.isEmpty() || shared || borrowed) return
        BufferPool.release(array)
        array = EMPTY_ARRAY
        readerIndex = 0; writerIndex = 0
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
        if (readerIndex != writerIndex) return
        readerIndex = 0
        writerIndex = 0
        when {
            shared || borrowed -> leaveArray()
            pooled && array.isNotEmpty() -> { BufferPool.release(array); array = EMPTY_ARRAY }
        }
    }

    /** Stop using a shared or borrowed array (never pooled); the next write takes a fresh one. Buffer must be empty. */
    private fun leaveArray() {
        array = EMPTY_ARRAY
        shared = false
        borrowed = false
    }

    private fun allocate(min: Int): ByteArray =
        if (pooled) BufferPool.acquire(min) else ByteArray(min)

    /** Copy the unread bytes into [dst] and switch to it, releasing the old array when it is ours alone. */
    private fun moveTo(dst: ByteArray) {
        val n = readableBytes
        array.copyInto(dst, 0, readerIndex, writerIndex)
        if (pooled && !shared && !borrowed && array.isNotEmpty()) BufferPool.release(array)
        array = dst
        readerIndex = 0
        writerIndex = n
        shared = false
        borrowed = false
    }

    private fun ensureWritable(length: Int) {
        // Past writerIndex is ours even when slices share the array (they only cover read space).
        if (!borrowed && writerIndex + length <= array.size) return
        if (array.isEmpty()) {                  // nothing held (pooled, or left a shared array)
            array = allocate(maxOf(firstCapacity, length))
            return
        }
        if (readerIndex > 0 && !shared && !borrowed) {
            discardReadBytes()
            if (writerIndex + length <= array.size) return
        }
        val need = readableBytes + length
        var newCapacity = maxOf(array.size, 16)
        while (newCapacity < need) newCapacity *= 2
        moveTo(allocate(newCapacity))
    }

    companion object {
        const val DEFAULT_CAPACITY = 1024
        private val EMPTY_ARRAY = ByteArray(0)

        /** A read-only buffer over [src]'s bytes without copying (for vectored writes of slices). */
        internal fun wrap(src: Bytes): Buffer {
            val b = Buffer(src.array, pooled = false, firstCapacity = DEFAULT_CAPACITY)
            b.readerIndex = src.offset
            b.writerIndex = src.offset + src.size
            b.borrowed = true
            return b
        }
    }
}
