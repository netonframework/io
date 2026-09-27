@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.core.IoStream

/**
 * Raw byte-echo server: read into a per-connection buffer, write it straight back.
 *
 * There is no codec, no framing and no per-echo allocation — this measures the reactor and
 * the [IoStream] read/write path itself. One coroutine per connection, all on one reactor.
 *
 * Usage: echoServer [host=0.0.0.0] [port=9000] [reactors=1 | NETON_IO_REACTORS]
 * A host of `unix:/path` listens on that Unix domain socket instead (the port is ignored; SPEC §23.6).
 *
 * NETON_IO_ECHO_MODE: `raw` (default) echoes bytes; `lines` echoes newline-framed requests through
 * Framed + serve() (batched flush, SPEC §23.2); `lines-unbatched` does the same with one flush per
 * request (the pre-§23.2 behaviour, kept here for comparison only).
 *
 * NETON_IO_POOL=0 turns buffer pooling off (SPEC §23.7), for paired comparisons.
 * NETON_IO_RUN_SECONDS=n stops the server after n seconds (orderly, so NETON_IO_STATS is printed);
 * otherwise it runs until killed.
 */
fun echoServerMain(args: Array<String>) {
    val host = args.getOrNull(0) ?: "0.0.0.0"
    val port = args.getOrNull(1)?.toIntOrNull() ?: 9000
    val reactors = args.getOrNull(2)?.toIntOrNull()
        ?: platform.posix.getenv("NETON_IO_REACTORS")?.toKString()?.toIntOrNull()
        ?: 1
    val runSeconds = platform.posix.getenv("NETON_IO_RUN_SECONDS")?.toKString()?.toIntOrNull()
    // Bench knob (SPEC §17c): fix the GC target heap instead of letting the runtime autotune it.
    GcTuning.fromEnvironment()?.let { mb -> println("gc target heap = $mb MiB (autotune off)") }
    if (!pooling) neton.io.bytes.BufferPoolConfig.enabled = false
    println("echo-server listening on $host:$port reactors=$reactors accept=${platform.posix.getenv("NETON_IO_ACCEPT_MODE")?.toKString() ?: "handoff"}")
    val unixPath = host.removePrefix("unix:").takeIf { host.startsWith("unix:") }
    if (unixPath != null && reactors > 1) {
        runReactor {
            val group = listenUnixGroup(unixPath, reactors)
            if (runSeconds != null) launch { delay(runSeconds * 1000L); group.close() }
            group.serve { conn -> echoConnection(conn) }
            group.awaitWorkers()
        }
    } else if (reactors <= 1) {
        runReactor {
            val accept: suspend () -> IoStream
            val close: () -> Unit
            if (unixPath != null) { val l = listenUnix(unixPath); accept = { l.accept() }; close = { l.close() } }
            else { val l = listen(host, port); accept = { l.accept() }; close = { l.close() } }
            if (runSeconds != null) launch { delay(runSeconds * 1000L); close() }
            try {
                while (true) {
                    val conn = accept()
                    launch { echoConnection(conn) }
                }
            } catch (_: neton.io.core.ClosedException) {
            }
        }
    } else {
        val until = runSeconds?.let { secs ->
            CompletableDeferred<Unit>().also { d ->
                kotlin.native.concurrent.Worker.start(name = "echo-timer").executeAfter(secs * 1_000_000L) { d.complete(Unit) }
            }
        }
        // Bench knob (SPEC §23.4): NETON_IO_ACCEPT_MODE=reuseport gives every reactor its own listener.
        val mode = if (platform.posix.getenv("NETON_IO_ACCEPT_MODE")?.toKString() == "reuseport") AcceptMode.ReusePort else AcceptMode.Handoff
        serveTcp(host, port, reactors, until, mode) { conn -> echoConnection(conn) }
    }
}

private val pooling: Boolean = platform.posix.getenv("NETON_IO_POOL")?.toKString() != "0"

private val echoMode: String =
    platform.posix.getenv("NETON_IO_ECHO_MODE")?.toKString() ?: "raw"

private suspend fun echoConnection(conn: IoStream) {
    when (echoMode) {
        "lines" -> return echoLines(conn, batched = true)
        "lines-unbatched" -> return echoLines(conn, batched = false)
    }
    // Default initial capacity, grown on demand (SPEC §19.4). Pooled unless NETON_IO_POOL=0 (SPEC §23.7):
    // an idle connection then holds no buffer while its read is parked.
    val buf = Buffer(pooled = pooling)
    try {
        while (true) {
            buf.clear()
            val n = conn.read(buf)
            if (n < 0) break
            conn.write(buf)
        }
    } finally {
        conn.close()
    }
}

private suspend fun echoLines(conn: IoStream, batched: Boolean) {
    val framed = neton.io.core.Framed(neton.io.core.Io(conn), neton.io.codec.LineCodec, neton.io.codec.LineCodec)
    try {
        if (batched) neton.io.core.serve(framed, neton.io.core.Service<String, String> { it })
        else framed.incoming().collect { framed.send(it) }
    } catch (_: neton.io.core.IoException) {
    } finally {
        conn.close()
    }
}
