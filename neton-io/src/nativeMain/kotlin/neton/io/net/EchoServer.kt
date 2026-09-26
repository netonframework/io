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
 *
 * NETON_IO_ECHO_MODE: `raw` (default) echoes bytes; `lines` echoes newline-framed requests through
 * Framed + serve() (batched flush, SPEC §23.2); `lines-unbatched` does the same with one flush per
 * request (the pre-§23.2 behaviour, kept here for comparison only).
 *
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
    platform.posix.getenv("NETON_IO_GC_TARGET_MB")?.toKString()?.toLongOrNull()?.let { mb ->
        @OptIn(kotlin.native.runtime.NativeRuntimeApi::class)
        run { kotlin.native.runtime.GC.autotune = false; kotlin.native.runtime.GC.targetHeapBytes = mb shl 20 }
        println("gc target heap = $mb MiB (autotune off)")
    }
    println("echo-server listening on $host:$port reactors=$reactors accept=${platform.posix.getenv("NETON_IO_ACCEPT_MODE")?.toKString() ?: "handoff"}")
    if (reactors <= 1) {
        runReactor {
            val server = listen(host, port)
            if (runSeconds != null) launch { delay(runSeconds * 1000L); server.close() }
            try {
                while (true) {
                    val conn = server.accept()
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

private val echoMode: String =
    platform.posix.getenv("NETON_IO_ECHO_MODE")?.toKString() ?: "raw"

private suspend fun echoConnection(conn: IoStream) {
    when (echoMode) {
        "lines" -> return echoLines(conn, batched = true)
        "lines-unbatched" -> return echoLines(conn, batched = false)
    }
    // Default initial capacity, grown on demand (SPEC §19.4) — geario's echo holds buffers the same way.
    val buf = Buffer()
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
