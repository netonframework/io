package neton.io.bytes

@kotlin.native.concurrent.ThreadLocal
internal actual val threadPool: BufferPool = BufferPool()
