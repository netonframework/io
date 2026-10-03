@file:OptIn(ObsoleteWorkersApi::class)

package neton.io.net

import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker

internal actual fun startTestWorker(name: String): TestWorker = object : TestWorker {
    private val worker = Worker.start(name = name)

    override fun <T> submit(block: () -> T): () -> T {
        val future = worker.execute(TransferMode.SAFE, { block }) { it() }
        return { future.result }
    }

    override fun submitAfter(delayMicros: Long, block: () -> Unit) = worker.executeAfter(delayMicros, block)

    override fun stop() { worker.requestTermination(processScheduledJobs = true).result }
}
