@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString

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
 */
fun echoServerMain(args: Array<String>) {
    val host = args.getOrNull(0) ?: "0.0.0.0"
    val port = args.getOrNull(1)?.toIntOrNull() ?: 9000
    val reactors = args.getOrNull(2)?.toIntOrNull()
        ?: platform.posix.getenv("NETON_IO_REACTORS")?.toKString()?.toIntOrNull()
        ?: 1
    println("echo-server listening on $host:$port reactors=$reactors")
    if (reactors <= 1) {
        runReactor {
            val server = listen(host, port)
            while (true) {
                val conn = server.accept()
                launch { echoConnection(conn) }
            }
        }
    } else {
        serveTcp(host, port, reactors) { conn -> echoConnection(conn) }
    }
}

private suspend fun echoConnection(conn: IoStream) {
    val buf = Buffer(64 * 1024)
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
