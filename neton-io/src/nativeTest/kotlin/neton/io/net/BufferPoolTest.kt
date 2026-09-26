package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.bytes.BufferPool
import neton.io.bytes.Bytes
import neton.io.core.IoStream
import neton.io.core.writev
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** SPEC §23.7: per-thread buffer pool, pooled Buffer, zero-copy Bytes slices. */
class BufferPoolTest {

    private fun bytes(n: Int, seed: Int = 0) = ByteArray(n) { ((it * 7 + seed) and 0xFF).toByte() }

    @Test
    fun pooledBufferTakesAnArrayOnlyWhileItHoldsData() {
        BufferPool.clear()
        val b = Buffer(pooled = true)
        assertEquals(0, b.capacity, "no array before the first write")
        b.writeBytes(bytes(100))
        assertEquals(BufferPool.MIN_SIZE, b.capacity, "smallest size class")
        val first = b.backingArray()
        b.skip(100)                                        // drained: the array goes back
        assertEquals(0, b.capacity)
        assertEquals(1, BufferPool.returned)
        b.writeByte(1)
        assertTrue(b.backingArray() === first, "the same array comes back (LIFO)")
        assertEquals(1, BufferPool.hits)
    }

    @Test
    fun growthMovesUpAClassAndReturnsTheOldArray() {
        BufferPool.clear()
        val b = Buffer(pooled = true)
        val data = bytes(10_000)
        b.writeBytes(data, 0, 1000)
        b.writeBytes(data, 1000, 9000)                     // 2 KiB -> 16 KiB
        assertEquals(16 * 1024, b.capacity)
        assertTrue(BufferPool.returned >= 1, "outgrown array returned")
        assertContentEquals(data, b.readAll())
    }

    @Test
    fun oversizedArraysAreNotCached() {
        BufferPool.clear()
        val b = Buffer(pooled = true)
        b.writeBytes(bytes(200_000))
        b.skip(200_000)
        assertEquals(0, BufferPool.cachedBytesNow)
        assertTrue(BufferPool.dropped >= 1)
    }

    @Test
    fun releaseIfIdleOnlyReleasesAnEmptyBuffer() {
        BufferPool.clear()
        val b = Buffer(pooled = true)
        b.reserve(10)
        b.releaseIfIdle()
        assertEquals(0, b.capacity)
        b.writeBytes(bytes(10))
        b.releaseIfIdle()
        assertEquals(BufferPool.MIN_SIZE, b.capacity, "data still unread: array kept")
        val plain = Buffer(64)
        plain.releaseIfIdle()
        assertEquals(64, plain.capacity, "unpooled buffers are untouched")
    }

    /** A slice never changes: not by later writes, compaction, clear, reuse, or pool recycling. */
    @Test
    fun slicesStayIntactWhateverTheBufferDoesNext() {
        for (pooled in listOf(false, true)) {
            BufferPool.clear()
            val b = Buffer(64, pooled = pooled)
            val src = bytes(3000, seed = 1)
            b.writeBytes(src, 0, 40)
            val s1 = b.readSlice(10)                        // shares the array; 30 bytes left
            b.writeBytes(src, 40, 20)                       // fits after writerIndex: same array, slice untouched
            b.discardReadBytes()                            // would move data over s1: must use a new array
            val s2 = b.readSlice(20)
            b.writeBytes(src, 60, 2000)                     // growth
            b.clear()
            b.writeBytes(bytes(3000, seed = 99))            // reuse after clear
            b.skip(b.readableBytes)                         // drain: the shared array must not be pooled
            repeat(4) { val other = Buffer(pooled = true); other.writeBytes(bytes(3000, seed = 5)); other.skip(3000) }
            assertContentEquals(src.copyOfRange(0, 10), s1.toByteArray(), "s1 (pooled=$pooled)")
            assertContentEquals(src.copyOfRange(10, 30), s2.toByteArray(), "s2 (pooled=$pooled)")
            assertEquals(Bytes.copyOf(src, 13, 17), s2.slice(3, 7))
        }
    }

    @Test
    fun readSliceIsZeroCopy() {
        val b = Buffer(64)
        b.writeBytes(bytes(32))
        val arr = b.backingArray()
        val s = b.readSlice(16)
        assertTrue(s.array === arr, "slice points into the buffer's array")
        assertEquals(0, s.offset)
        assertEquals(bytes(32)[4], s[4])
        val t = b.readSlice(16)
        assertTrue(t.array === arr && t.offset == 16, "second slice follows in the same array")
    }

    @Test
    fun slicesAreWrittenWithoutCopying() = runReactor {
        val listener = listen("127.0.0.1", 21920)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(listener.accept()) }
        val c = connect("127.0.0.1", 21920)
        val s = accepted.await()
        val src = Buffer(1 shl 20)
        val payload = bytes(1 shl 20, seed = 3)
        src.writeBytes(payload)
        val slices = ArrayList<Bytes>()
        var left = payload.size
        var k = 0
        while (left > 0) { val n = minOf(left, 1000 + (k++ * 37) % 5000); slices.add(src.readSlice(n)); left -= n }
        val reader = async { delay(20); val got = Buffer(); while (got.readableBytes < payload.size) { if (s.read(got) < 0) break }; got.readAll() }
        assertEquals(payload.size.toLong(), c.writev(slices))
        assertContentEquals(payload, reader.await())
        assertEquals(slices.sumOf { it.size }, payload.size, "slices unchanged by the write")
        c.close(); s.close(); listener.close()
    }

    /** An idle pooled read on the readiness drivers holds no array while parked. */
    @Test
    fun parkedReadHoldsNoPooledArray() = runReactor {
        val listener = listen("127.0.0.1", 21921)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(listener.accept()) }
        val c = connect("127.0.0.1", 21921)
        val s = accepted.await()
        val buf = Buffer(pooled = true)
        val r = async { s.read(buf) }
        delay(100)
        // io_uring's single-shot read lends the array to the kernel while parked; multishot and the
        // readiness drivers hold none.
        if (currentReactor()::class.simpleName == "UringReactor" && buf.capacity != 0) println("SKIP idle-release check: io_uring single-shot read owns the array")
        else assertEquals(0, buf.capacity, "parked read must not hold an array")
        val msg = Buffer(); msg.writeBytes(bytes(10)); c.write(msg)
        assertEquals(10, r.await())
        c.close(); s.close(); listener.close()
    }
}
