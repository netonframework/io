package neton.io.net

internal actual fun envVar(name: String): String? = System.getenv(name)

internal actual fun writeStderrLine(line: String) {
    System.err.println(line)
    System.err.flush()
}

internal actual fun sleepMicros(micros: Int) {
    Thread.sleep(micros / 1000L, (micros % 1000) * 1000)
}

/**
 * Daemon threads: a worker reactor never keeps the VM alive on its own. A group that is shut down
 * properly waits for them anyway ([ReactorGroup.awaitExit]). A JVM thread ends when its body
 * returns, so the stop request has nothing to do.
 */
internal actual fun startReactorThread(name: String, body: () -> Unit): () -> Unit {
    val thread = Thread(body, name)
    thread.isDaemon = true
    thread.start()
    return {}
}

/** The JDK has no thread affinity API. */
internal actual fun pinCurrentThread(n: Int): Int = -1

internal actual fun captureAffinity(): Int = -1

@Suppress("DEPRECATION")   // Thread.threadId() is Java 19; Android has only getId()
internal actual fun currentThreadId(): ULong = Thread.currentThread().id.toULong()

actual fun cpuCount(): Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)

/** SO_REUSEPORT load balancing is a Linux kernel feature the JDK does not expose portably. */
internal actual val reusePortBalancesLoad: Boolean = false

internal actual val connectionResetErrno: Int = JvmErrno.ECONNRESET

internal actual fun createReactor(): Reactor = NioReactor()
