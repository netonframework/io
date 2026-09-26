package neton.io.bytes

import kotlin.concurrent.Volatile

/**
 * Settings for the per-thread [BufferPool] (SPEC §23.7). Set them before starting reactors: each
 * thread's pool reads them once, on its first use ([BufferPool.clear] re-reads them).
 */
object BufferPoolConfig {
    /** When false, pooled buffers allocate and drop arrays like plain ones. */
    @Volatile var enabled: Boolean = true
    /** Arrays cached per size class, per thread. */
    @Volatile var maxArraysPerClass: Int = 128
    /** Bytes cached across all size classes, per thread. */
    @Volatile var maxBytesPerThread: Int = 4 shl 20
}

/**
 * Per-thread cache of byte arrays in power-of-two size classes from 2 KiB to 64 KiB (SPEC §23.7).
 * One instance per thread ([current]), so no locking: an array released on another thread than the
 * one that acquired it simply joins that thread's cache. Larger requests are allocated and never
 * cached. Reactors look their thread's pool up once and pass it on their hot paths.
 */
class BufferPool internal constructor() {
    companion object {
        const val MIN_CLASS_SHIFT = 11           // 2 KiB
        const val MAX_CLASS_SHIFT = 16           // 64 KiB
        const val MIN_SIZE = 1 shl MIN_CLASS_SHIFT
        const val MAX_SIZE = 1 shl MAX_CLASS_SHIFT
        private const val CLASSES = MAX_CLASS_SHIFT - MIN_CLASS_SHIFT + 1

        /** The calling thread's pool. */
        val current: BufferPool get() = threadPool

        /** Smallest size class holding [min] bytes, or [min] itself above [MAX_SIZE]. */
        fun sizeFor(min: Int): Int {
            if (min > MAX_SIZE) return min
            var s = MIN_SIZE
            while (s < min) s = s shl 1
            return s
        }
    }

    private val stacks = Array(CLASSES) { arrayOfNulls<ByteArray>(0) }
    private val counts = IntArray(CLASSES)
    private var cachedBytes = 0
    // BufferPoolConfig read once per thread (it is set before reactors start), not on every call.
    private var enabled = BufferPoolConfig.enabled
    private var maxPerClass = BufferPoolConfig.maxArraysPerClass
    private var maxBytes = BufferPoolConfig.maxBytesPerThread

    /** Counters for tests and diagnostics (this thread only). */
    var hits = 0L; private set
    var misses = 0L; private set
    var returned = 0L; private set
    var dropped = 0L; private set
    val cachedBytesNow: Int get() = cachedBytes

    /** An array of at least [min] bytes (exactly [sizeFor] bytes); contents are unspecified. */
    fun acquire(min: Int): ByteArray {
        if (min > MAX_SIZE || !enabled) return ByteArray(sizeFor(min))
        val c = classOf(min)
        val n = counts[c]
        if (n > 0) {
            val stack = stacks[c]
            val a = stack[n - 1]!!
            stack[n - 1] = null
            counts[c] = n - 1
            cachedBytes -= a.size
            hits++
            return a
        }
        misses++
        return ByteArray(MIN_SIZE shl c)
    }

    /** Give [array] back. Arrays that are not a size class, or over the caps, are left to the GC. */
    fun release(array: ByteArray) {
        val size = array.size
        if (!enabled || size < MIN_SIZE || size > MAX_SIZE || size and (size - 1) != 0) { dropped++; return }
        val c = size.countTrailingZeroBits() - MIN_CLASS_SHIFT
        val n = counts[c]
        if (n >= maxPerClass || cachedBytes + size > maxBytes) { dropped++; return }
        var stack = stacks[c]
        if (n == stack.size) { stack = stack.copyOf(maxOf(8, n * 2).coerceAtMost(maxPerClass)); stacks[c] = stack }
        stack[n] = array
        counts[c] = n + 1
        cachedBytes += size
        returned++
    }

    /** Drop every cached array (tests). */
    fun clear() {
        enabled = BufferPoolConfig.enabled; maxPerClass = BufferPoolConfig.maxArraysPerClass; maxBytes = BufferPoolConfig.maxBytesPerThread
        for (c in 0 until CLASSES) { stacks[c] = arrayOfNulls(0); counts[c] = 0 }
        cachedBytes = 0
        hits = 0; misses = 0; returned = 0; dropped = 0
    }

    /** Size class holding [min] (<= MAX_SIZE) bytes: ceil(log2(min)) - MIN_CLASS_SHIFT, at least 0. */
    private fun classOf(min: Int): Int =
        if (min <= MIN_SIZE) 0 else (32 - (min - 1).countLeadingZeroBits()) - MIN_CLASS_SHIFT
}

@kotlin.native.concurrent.ThreadLocal
private val threadPool = BufferPool()
