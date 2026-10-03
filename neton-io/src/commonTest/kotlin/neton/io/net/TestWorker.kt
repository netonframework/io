package neton.io.net

/**
 * "Another thread" for tests of the cross-thread contract (dispatch, cancellation, posting racing a
 * close). A Kotlin/Native Worker on native, a single-thread executor on the JVM; jobs run in order.
 */
internal interface TestWorker {
    /** Run [block] on the worker; the returned function waits for it and returns its result. */
    fun <T> submit(block: () -> T): () -> T

    /** Run [block] on the worker after [delayMicros]. */
    fun submitAfter(delayMicros: Long, block: () -> Unit)

    /** Finish the queued and scheduled jobs, then end the thread; returns once it has. */
    fun stop()
}

internal expect fun startTestWorker(name: String = "test-worker"): TestWorker
