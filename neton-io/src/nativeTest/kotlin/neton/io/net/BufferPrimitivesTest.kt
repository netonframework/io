package neton.io.net

import neton.io.bytes.Buffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** Big-endian primitives on Buffer (SPEC §24.10). */
class BufferPrimitivesTest {
    @Test
    fun writesAreBigEndianAndReadsRoundTrip() {
        val b = Buffer(16)
        b.writeByte(0x7f); b.writeShort(0xABCD); b.writeInt(0x01020304); b.writeLong(-2L); b.writeInt(-1)
        assertContentEquals(byteArrayOf(0x7f, 0xAB.toByte(), 0xCD.toByte(), 1, 2, 3, 4,
            -1, -1, -1, -1, -1, -1, -1, -2, -1, -1, -1, -1), b.peekAll())
        assertEquals(0x7f, b.getUnsignedByte(0))
        assertEquals(0xABCD, b.getUnsignedShort(1))
        assertEquals(0x01020304, b.getInt(3))
        b.skip(3)
        assertEquals(0x01020304, b.readInt())
        assertEquals(-1, b.readInt()); assertEquals(-2, b.readInt())
        assertEquals(0xFFFF, b.readUnsignedShort()); assertEquals(0xFFFF, b.readUnsignedShort())
        assertEquals(0, b.readableBytes)
    }

    @Test
    fun readsBeyondTheReadableRegionThrow() {
        val b = Buffer(8); b.writeShort(1)
        assertFailsWith<IndexOutOfBoundsException> { b.getInt(0) }
        assertFailsWith<IndexOutOfBoundsException> { b.getUnsignedShort(1) }
        assertFailsWith<IndexOutOfBoundsException> { b.getUnsignedByte(2) }
    }

    @Test
    fun writeByteGrowsAndWorksOnPooledBuffers() {
        val b = Buffer(pooled = true)
        repeat(5000) { b.writeByte((it and 0x7f).toByte()) }
        assertEquals(5000, b.readableBytes)
        assertEquals(4999 and 0x7f, b.getUnsignedByte(4999))
    }
}
