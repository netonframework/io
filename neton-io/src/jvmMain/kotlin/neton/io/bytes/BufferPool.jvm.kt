package neton.io.bytes

// A ThreadLocal subclass rather than ThreadLocal.withInitial, which Android only has from API 26.
private val pools = object : ThreadLocal<BufferPool>() {
    override fun initialValue(): BufferPool = BufferPool()
}

internal actual val threadPool: BufferPool get() = pools.get()
