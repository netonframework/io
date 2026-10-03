package neton.io.net

import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

internal actual fun startTestWorker(name: String): TestWorker = object : TestWorker {
    private val executor = ScheduledThreadPoolExecutor(1) { r -> Thread(r, name).apply { isDaemon = true } }

    override fun <T> submit(block: () -> T): () -> T {
        val future = executor.submit(Callable { block() })
        return { try { future.get() } catch (e: ExecutionException) { throw e.cause ?: e } }
    }

    override fun submitAfter(delayMicros: Long, block: () -> Unit) {
        executor.schedule({ block() }, delayMicros, TimeUnit.MICROSECONDS)
    }

    // shutdown() still runs the delayed jobs already scheduled, as requestTermination does on native.
    override fun stop() {
        executor.shutdown()
        check(executor.awaitTermination(60, TimeUnit.SECONDS)) { "test worker $name did not finish" }
    }
}
