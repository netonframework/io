package neton.io.bytes

import neton.io.codec.LineCodec
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Random operation sequences on [Buffer], checked against a plain list of bytes: every protocol parser stands on its
 * cursors, compaction, growth, zero-copy slices, borrowed views and pooled arrays. From fixed seeds (reproducible).
 * After every operation the readable bytes must equal the model's, every slice handed out must still hold the bytes it
 * had (slices share the array: nothing may write over them), and a borrowed source must never be written into.
 */
class BufferFuzzTest {
    private class Slice(val bytes: Bytes, val expected: ByteArray)

    private fun run(seed: Int, pooled: Boolean) {
        val rng = Random(seed)
        val buf = Buffer(rng.nextInt(0, 64), pooled)
        val model = ArrayList<Byte>()
        val slices = ArrayList<Slice>()
        var borrowed: Pair<ByteArray, ByteArray>? = null // the source and a copy of it as lent

        repeat(OPERATIONS) { step ->
            val readable = model.size
            when (rng.nextInt(16)) {
                0 -> { val b = rng.nextInt(256).toByte(); buf.writeByte(b); model += b }
                1 -> {
                    val src = rng.nextBytes(rng.nextInt(0, 3000))
                    val off = rng.nextInt(0, src.size + 1); val len = rng.nextInt(0, src.size - off + 1)
                    buf.writeBytes(src, off, len); for (i in off until off + len) model += src[i]
                }
                2 -> {
                    val src = rng.nextBytes(rng.nextInt(0, 200))
                    val off = rng.nextInt(0, src.size + 1); val len = rng.nextInt(0, src.size - off + 1)
                    buf.writeBytes(Bytes(src, off, len)); for (i in off until off + len) model += src[i]
                }
                3 -> { val v = rng.nextInt(); buf.writeInt(v); repeat(4) { model += (v ushr (24 - 8 * it)).toByte() } }
                4 -> {
                    val min = rng.nextInt(0, 2000)
                    val room = buf.reserve(min)
                    assertTrue(room >= min, "reserve($min) gave $room")
                    val n = rng.nextInt(0, room + 1)
                    val a = buf.backingArray(); val w = buf.writerIndex()
                    repeat(n) { val b = rng.nextInt(256).toByte(); a[w + it] = b; model += b }
                    buf.commitWrite(n)
                }
                5 -> { val n = rng.nextInt(0, readable + 1); assertContentEquals(model.take(n).toByteArray(), buf.readBytes(n)); model.subList(0, n).clear() }
                6 -> {
                    val n = rng.nextInt(0, readable + 1)
                    val s = buf.readSlice(n)
                    val expected = model.take(n).toByteArray()
                    assertContentEquals(expected, s.toByteArray()); model.subList(0, n).clear()
                    slices += Slice(s, expected)
                    if (slices.size > 32) slices.removeAt(rng.nextInt(slices.size))
                }
                7 -> { val n = rng.nextInt(0, readable + 1); buf.skip(n); model.subList(0, n).clear() }
                8 -> { val n = rng.nextInt(0, readable + 1); buf.consume(n); model.subList(0, n).clear() }
                9 -> buf.discardReadBytes()
                10 -> if (rng.nextInt(8) == 0) { buf.clear(); model.clear() }
                11 -> buf.releaseIfIdle()
                12 -> if (readable >= 4) {
                    val v = buf.readInt()
                    assertEquals(model.take(4).fold(0) { acc, b -> (acc shl 8) or (b.toInt() and 0xff) }, v); model.subList(0, 4).clear()
                }
                13 -> if (!pooled && rng.nextInt(4) == 0) {
                    val src = rng.nextBytes(rng.nextInt(0, 300))
                    val off = rng.nextInt(0, src.size + 1); val len = rng.nextInt(0, src.size - off + 1)
                    buf.borrow(Bytes(src, off, len))
                    model.clear(); for (i in off until off + len) model += src[i]
                    borrowed = src to src.copyOf()
                }
                14 -> {
                    // LineCodec over whatever is buffered: a line ends at the first '\n'.
                    if (rng.nextBoolean()) { LineCodec.encode("l$step", buf); model.addAll("l$step\n".encodeToByteArray().toList()) }
                    val nl = model.indexOf('\n'.code.toByte())
                    val line = LineCodec.decode(buf)
                    if (nl < 0) assertEquals(null, line) else {
                        assertEquals(model.take(nl).toByteArray().decodeToString(), line); model.subList(0, nl + 1).clear()
                    }
                }
                else -> if (readable > 0) {
                    val i = rng.nextInt(readable)
                    assertEquals(model[i], buf.getByte(i))
                    assertEquals(model.indexOf(model[i]), buf.indexOf(model[i]))
                }
            }
            assertEquals(model.size, buf.readableBytes, "step $step")
            if (step % 16 == 0 || model.size < 64) assertContentEquals(model.toByteArray(), buf.peekAll(), "step $step")
            for (s in slices) assertContentEquals(s.expected, s.bytes.toByteArray(), "a slice was overwritten at step $step")
            borrowed?.let { (src, copy) -> assertContentEquals(copy, src, "a borrowed source was written into at step $step") }
        }
    }

    @Test
    fun unpooled() {
        for (seed in 0 until SEEDS) run(seed, pooled = false)
    }

    @Test
    fun pooled() {
        for (seed in 0 until SEEDS) run(1000 + seed, pooled = true)
    }

    private companion object {
        const val SEEDS = 12
        const val OPERATIONS = 2_000
    }
}
