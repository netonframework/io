package neton.io.net

import neton.io.core.secureRandom
import neton.io.core.secureRandomLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** SPEC §28.15: the OS CSPRNG. */
class SecureRandomTest {
    @Test
    fun fillsTheRangeOnlyAndDiffersEachTime() {
        val a = ByteArray(64); val b = ByteArray(64)
        secureRandom(a, 8, 48); secureRandom(b, 8, 48)
        assertTrue((0 until 8).all { a[it] == 0.toByte() } && (56 until 64).all { a[it] == 0.toByte() }, "wrote outside the range")
        assertFalse(a.copyOfRange(8, 56).contentEquals(b.copyOfRange(8, 56)), "two draws were equal")
        // 1 MiB: every byte value appears, and roughly uniformly (a gross-failure check, not a statistical test).
        val big = ByteArray(1 shl 20); secureRandom(big)
        val counts = IntArray(256); big.forEach { counts[it.toInt() and 0xFF]++ }
        assertTrue(counts.all { it in 3000..5200 }, "byte frequencies far from uniform: ${counts.minOrNull()}..${counts.maxOrNull()}")
        assertTrue(secureRandomLong() != secureRandomLong())
        secureRandom(ByteArray(0))
        assertFailsWith<IllegalArgumentException> { secureRandom(ByteArray(4), 2, 3) }
        assertEquals(4, ByteArray(4).also { secureRandom(it, 4, 0) }.size)
    }
}
