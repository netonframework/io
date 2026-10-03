@file:OptIn(ExperimentalForeignApi::class, ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker

internal actual fun envVar(name: String): String? = platform.posix.getenv(name)?.toKString()

internal actual fun writeStderrLine(line: String) {
    platform.posix.fprintf(platform.posix.stderr, "%s\n", line)
    platform.posix.fflush(platform.posix.stderr)
}

internal actual fun sleepMicros(micros: Int) {
    platform.posix.usleep(micros.toUInt())
}

internal actual fun startReactorThread(name: String, body: () -> Unit): () -> Unit {
    val worker = Worker.start(name = name)
    worker.execute(TransferMode.SAFE, { body }) { it() }
    return { worker.requestTermination(processScheduledJobs = true) }
}
