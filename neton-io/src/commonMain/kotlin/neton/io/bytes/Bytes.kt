package neton.io.bytes

/**
 * An immutable byte slice: a window onto an array that nobody writes again (SPEC §23.7).
 *
 * [Buffer.readSlice] hands one out without copying; the buffer then treats that array as shared and
 * never writes into the sliced region again (it moves to a fresh array instead), so a slice's
 * content is fixed for its whole life. Content equality, like `ByteArray.contentEquals`.
 */
class Bytes internal constructor(internal val array: ByteArray, internal val offset: Int, val size: Int) {

    val isEmpty: Boolean get() = size == 0

    operator fun get(i: Int): Byte {
        if (i !in 0 until size) throw IndexOutOfBoundsException("index $i, size $size")
        return array[offset + i]
    }

    /** A sub-slice `[from, to)`, sharing the same array (no copy). */
    fun slice(from: Int, to: Int = size): Bytes {
        require(from in 0..to && to <= size) { "bad range [$from, $to) of $size" }
        return if (from == 0 && to == size) this else Bytes(array, offset + from, to - from)
    }

    /** Index of [b] in this slice, or -1. */
    fun indexOf(b: Byte): Int {
        for (i in 0 until size) if (array[offset + i] == b) return i
        return -1
    }

    /** A copy of the bytes. */
    fun toByteArray(): ByteArray = array.copyOfRange(offset, offset + size)

    fun copyInto(dst: ByteArray, dstOffset: Int = 0) { array.copyInto(dst, dstOffset, offset, offset + size) }

    fun decodeToString(): String = array.decodeToString(offset, offset + size)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Bytes || other.size != size) return false
        for (i in 0 until size) if (array[offset + i] != other.array[other.offset + i]) return false
        return true
    }

    override fun hashCode(): Int {
        var h = 1
        for (i in 0 until size) h = 31 * h + array[offset + i]
        return h
    }

    override fun toString(): String = "Bytes(size=$size)"

    companion object {
        val EMPTY = Bytes(ByteArray(0), 0, 0)

        /** A slice holding a copy of [src] (later changes to [src] do not show). */
        fun copyOf(src: ByteArray, from: Int = 0, to: Int = src.size): Bytes = Bytes(src.copyOfRange(from, to), 0, to - from)

        /** A slice over [src] without copying; the caller promises never to modify [src] again. */
        fun wrap(src: ByteArray): Bytes = Bytes(src, 0, src.size)
    }
}
