package neton.io.bytes

/**
 * A growable byte buffer with separate reader and writer cursors.
 *
 * Semantics mirror a minimal `BytesMut`: append on write, consume on read, and reclaim the
 * already-read prefix so a long-lived buffer does not grow without bound.
 *
 * Pooled buffers (SPEC §23.7, `Buffer(pooled = true)`) take their array from the thread's
 * [BufferPool] on first write and give it back through [releaseIfIdle]: the reactors call it for
 * reads that stay parked (an idle sweep, SPEC §24), so an idle connection holds no buffer memory
 * while a busy one keeps the same array (and the same pin) across requests.
 *
 * [readSlice] returns a zero-copy [Bytes]. The array it points into is then *shared*: the buffer
 * never writes into already-read space of that array again (no compaction, no reset to 0); when it
 * would, it moves to a fresh array instead, and the shared one is left to the slices and the GC —
 * never returned to the pool.
 *
 * Reactors call the internal overloads taking their thread's [BufferPool]; everything else finds
 * the pool itself, and only when a pooled buffer actually takes or returns an array.
 */
class Buffer private constructor(
    private var array: ByteArray,
    /** [POOLED] | [SHARED] | [BORROWED]: one field, so the common path tests a single value. */
    private var mode: Int,
    /** Capacity of the first array taken lazily (pooled, or after leaving a shared array). */
    private val firstCapacity: Int,
) {
    constructor(initialCapacity: Int = DEFAULT_CAPACITY, pooled: Boolean = false) : this(
        if (pooled) EMPTY_ARRAY else ByteArray(initialCapacity.coerceAtLeast(16)),
        if (pooled) POOLED else 0,
        initialCapacity.coerceAtLeast(16),
    )

    private var readerIndex = 0
    private var writerIndex = 0

    /** Whether arrays come from, and go back to, the thread's [BufferPool]. */
    val pooled: Boolean get() = mode and POOLED != 0
    // SHARED: slices point into [array], already-read space must not be rewritten.
    // BORROWED: [array] belongs to someone else (a wrapped [Bytes]), nothing may be written into it.
    private val sharedOrBorrowed: Boolean get() = mode and (SHARED or BORROWED) != 0

    /** Number of bytes available to read. */
    val readableBytes: Int get() = writerIndex - readerIndex

    val isEmpty: Boolean get() = readableBytes == 0

    /** Size of the array currently held (0 for a pooled buffer that holds none). */
    val capacity: Int get() = array.size

    // ---- write ----

    fun writeByte(b: Byte) {
        val i = writerIndex
        if (i < array.size && mode and BORROWED == 0) { array[i] = b; writerIndex = i + 1; return }
        ensureWritable(1, null)
        array[writerIndex++] = b
    }

    // ---- big-endian (network order) primitives: one capacity / bounds check per value (SPEC §24.10).
    // Byte-at-a-time header codecs spent ~1,400 instructions per message in writeByte / getByte.

    /** Append the low 16 bits of [v], big-endian. */
    fun writeShort(v: Int) {
        ensureWritable(2, null)
        val a = array; val i = writerIndex
        a[i] = (v ushr 8).toByte(); a[i + 1] = v.toByte()
        writerIndex = i + 2
    }

    /** Append [v], big-endian. */
    fun writeInt(v: Int) {
        ensureWritable(4, null)
        val a = array; val i = writerIndex
        a[i] = (v ushr 24).toByte(); a[i + 1] = (v ushr 16).toByte(); a[i + 2] = (v ushr 8).toByte(); a[i + 3] = v.toByte()
        writerIndex = i + 4
    }

    /** Append [v], big-endian. */
    fun writeLong(v: Long) {
        writeInt((v ushr 32).toInt())
        writeInt(v.toInt())
    }

    /** Readable byte at [i] (from the reader cursor) as 0..255, without consuming. */
    fun getUnsignedByte(i: Int): Int {
        if (i < 0 || i >= readableBytes) throw IndexOutOfBoundsException("index $i, readable $readableBytes")
        return array[readerIndex + i].toInt() and 0xFF
    }

    /** Big-endian unsigned 16-bit value at [i] (from the reader cursor), without consuming. */
    fun getUnsignedShort(i: Int): Int {
        if (i < 0 || i + 2 > readableBytes) throw IndexOutOfBoundsException("index $i+2, readable $readableBytes")
        val a = array; val p = readerIndex + i
        return ((a[p].toInt() and 0xFF) shl 8) or (a[p + 1].toInt() and 0xFF)
    }

    /** Big-endian 32-bit value at [i] (from the reader cursor), without consuming. */
    fun getInt(i: Int): Int {
        if (i < 0 || i + 4 > readableBytes) throw IndexOutOfBoundsException("index $i+4, readable $readableBytes")
        val a = array; val p = readerIndex + i
        return ((a[p].toInt() and 0xFF) shl 24) or ((a[p + 1].toInt() and 0xFF) shl 16) or
            ((a[p + 2].toInt() and 0xFF) shl 8) or (a[p + 3].toInt() and 0xFF)
    }

    /** Read and consume a big-endian 32-bit value. */
    fun readInt(): Int { val v = getInt(0); skip(4); return v }

    /** Read and consume a big-endian unsigned 16-bit value. */
    fun readUnsignedShort(): Int { val v = getUnsignedShort(0); skip(2); return v }

    fun writeBytes(src: ByteArray, offset: Int = 0, length: Int = src.size - offset) {
        require(offset >= 0 && length >= 0 && offset + length <= src.size) { "bad range" }
        if (length == 0) return
        ensureWritable(length, null)
        src.copyInto(array, writerIndex, offset, offset + length)
        writerIndex += length
    }

    /** Append a copy of [src]. */
    fun writeBytes(src: Bytes) {
        if (src.size == 0) return
        ensureWritable(src.size, null)
        src.copyInto(array, writerIndex)
        writerIndex += src.size
    }

    // ---- read ----

    /** Read and consume [length] bytes, returning a fresh array. */
    fun readBytes(length: Int): ByteArray {
        require(length in 0..readableBytes) { "length=$length readable=$readableBytes" }
        val out = array.copyOfRange(readerIndex, readerIndex + length)
        readerIndex += length
        resetIfDrained(null)
        return out
    }

    /** Read and consume [length] bytes as a zero-copy slice (SPEC §23.7). */
    fun readSlice(length: Int): Bytes {
        require(length in 0..readableBytes) { "length=$length readable=$readableBytes" }
        if (length == 0) return Bytes.EMPTY
        val out = Bytes(array, readerIndex, length)
        mode = mode or SHARED
        readerIndex += length
        resetIfDrained(null)
        return out
    }

    /** Consume all readable bytes. */
    fun readAll(): ByteArray = readBytes(readableBytes)

    /** Skip [n] readable bytes. */
    fun skip(n: Int) {
        require(n in 0..readableBytes)
        readerIndex += n
        resetIfDrained(null)
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
        if (sharedOrBorrowed) leaveArray()
    }

    /** Move unread data to the front, reclaiming already-read space (into a fresh array if shared). */
    fun discardReadBytes() {
        if (readerIndex == 0) return
        if (sharedOrBorrowed) { moveTo(allocate(maxOf(array.size, readableBytes), null), null); return }
        array.copyInto(array, 0, readerIndex, writerIndex)
        writerIndex -= readerIndex
        readerIndex = 0
    }

    /**
     * Give a pooled buffer's array back if nothing is left to read, so a connection parked in a
     * read holds no memory. Drivers call this before parking; a no-op for unpooled buffers.
     */
    fun releaseIfIdle() = releaseIfIdle(null)

    internal fun releaseIfIdle(pool: BufferPool?) {
        if (mode != POOLED || readerIndex != writerIndex || array.isEmpty()) return
        (pool ?: BufferPool.current).release(array)
        array = EMPTY_ARRAY
        readerIndex = 0; writerIndex = 0
    }

    // ---- direct I/O (fill/drain the backing memory in place, no intermediate copy) ----

    /** Ensure at least [min] writable bytes and return the writable capacity now available. */
    fun reserve(min: Int): Int = reserve(min, null)

    internal fun reserve(min: Int, pool: BufferPool?): Int {
        ensureWritable(min, pool)
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
    fun consume(n: Int) = consume(n, null)

    internal fun consume(n: Int, pool: BufferPool?) {
        require(n in 0..readableBytes)
        readerIndex += n
        resetIfDrained(pool)
    }

    /** The drivers' write path: consume [n] sent bytes (same as [consume]; kept for the drivers' call sites). */
    internal fun consumeSent(n: Int) = consume(n, null)

    // A drained buffer keeps its array (SPEC §24): handing it back on every drain made each request
    // take a different array from the pool, and each new array needs a new pin (an allocation).
    // Pooled arrays go back through [releaseIfIdle] — explicitly, or from the reactor's sweep of
    // reads that stay parked.
    @Suppress("UNUSED_PARAMETER")
    private fun resetIfDrained(pool: BufferPool?) {
        if (readerIndex != writerIndex) return
        readerIndex = 0
        writerIndex = 0
        if (sharedOrBorrowed) leaveArray()
    }

    /** A pooled buffer with nothing to read that still holds an array (what an idle sweep can take back). */
    internal val holdsIdleArray: Boolean get() = mode == POOLED && readerIndex == writerIndex && array.isNotEmpty()

    /** Stop using a shared or borrowed array (never pooled); the next write takes a fresh one. Buffer must be empty. */
    private fun leaveArray() {
        array = EMPTY_ARRAY
        mode = mode and POOLED
    }

    private fun allocate(min: Int, pool: BufferPool?): ByteArray =
        if (mode and POOLED != 0) (pool ?: BufferPool.current).acquire(min) else ByteArray(min)

    /** Copy the unread bytes into [dst] and switch to it, releasing the old array when it is ours alone. */
    private fun moveTo(dst: ByteArray, pool: BufferPool?) {
        val n = readableBytes
        array.copyInto(dst, 0, readerIndex, writerIndex)
        if (mode == POOLED && array.isNotEmpty()) (pool ?: BufferPool.current).release(array)
        array = dst
        readerIndex = 0
        writerIndex = n
        mode = mode and POOLED
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun ensureWritable(length: Int, pool: BufferPool?) {
        // Past writerIndex is ours even when slices share the array (they only cover read space).
        if (writerIndex + length <= array.size && mode and BORROWED == 0) return
        growOrMove(length, pool)
    }

    private fun growOrMove(length: Int, pool: BufferPool?) {
        if (array.isEmpty()) {                  // nothing held (pooled, or left a shared array)
            array = allocate(maxOf(firstCapacity, length), pool)
            return
        }
        if (readerIndex > 0 && !sharedOrBorrowed) {
            discardReadBytes()
            if (writerIndex + length <= array.size) return
        }
        val need = readableBytes + length
        var newCapacity = maxOf(array.size, 16)
        while (newCapacity < need) newCapacity *= 2
        moveTo(allocate(newCapacity, pool), pool)
    }

    companion object {
        const val DEFAULT_CAPACITY = 1024
        private const val POOLED = 1
        private const val SHARED = 2
        private const val BORROWED = 4
        private val EMPTY_ARRAY = ByteArray(0)

        /** A read-only buffer over [src]'s bytes without copying (for vectored writes of slices). */
        internal fun wrap(src: Bytes): Buffer {
            val b = Buffer(src.array, BORROWED, DEFAULT_CAPACITY)
            b.readerIndex = src.offset
            b.writerIndex = src.offset + src.size
            return b
        }
    }
}
