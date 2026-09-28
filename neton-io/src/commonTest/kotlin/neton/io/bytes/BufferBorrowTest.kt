package neton.io.bytes

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BufferBorrowTest {
    @Test
    fun borrowIsAZeroCopyReadOnlyViewThatCanBeReused() {
        val src = "hello world".encodeToByteArray()
        val b = Buffer()
        b.borrow(Bytes.copyOf(src).slice(6))
        assertEquals(5, b.readableBytes)
        assertContentEquals("world".encodeToByteArray(), b.readAll())
        val shared = Bytes.copyOf(src)
        b.borrow(shared)
        b.writeBytes("!".encodeToByteArray())                    // moves to a fresh array
        assertContentEquals("hello world!".encodeToByteArray(), b.readAll())
        assertContentEquals(src, shared.toByteArray())          // the borrowed bytes are untouched
        b.borrow(Bytes.EMPTY)
        assertEquals(0, b.readableBytes)
    }

    @Test
    fun pooledBuffersCannotBorrow() {
        assertFailsWith<IllegalArgumentException> { Buffer(pooled = true).borrow(Bytes.EMPTY) }
    }
}
