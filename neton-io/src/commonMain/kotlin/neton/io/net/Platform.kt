package neton.io.net

// What the shared reactor needs from its platform beyond the socket and poller drivers. Native
// implements these over POSIX / Win32 and Kotlin/Native's runtime, the JVM over the JDK.

/** An environment variable, or null. The NETON_IO_* switches are read through this. */
internal expect fun envVar(name: String): String?

/** One line on standard error, flushed (diagnostics: stats, connection faults). */
internal expect fun writeStderrLine(line: String)

/** Block the calling thread for about [micros] microseconds (startup waits only, never on a reactor). */
internal expect fun sleepMicros(micros: Int)

/**
 * Start [body] on a new thread named [name] (a worker reactor of a [ReactorGroup]). Returns the
 * thread's stop request: it lets the thread end once [body] has returned, queued behind it and
 * never interrupting it.
 */
internal expect fun startReactorThread(name: String, body: () -> Unit): () -> Unit

/** Pin the calling thread to the [n]-th CPU (modulo) of the captured set; the CPU, or -1 (SPEC §27.2). */
internal expect fun pinCurrentThread(n: Int): Int

/** Capture the process's allowed CPU set before any thread is pinned; its size, or -1 where unsupported. */
internal expect fun captureAffinity(): Int
