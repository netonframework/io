package neton.io.net

import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.core.IoStream

/**
 * Raw byte-echo server: read into a per-connection buffer, write it straight back.
 *
 * There is no codec, no framing and no per-echo allocation — this measures the reactor and
 * the [IoStream] read/write path itself. One coroutine per connection, all on one reactor.
 *
 * Usage: echoServer [host=0.0.0.0] [port=9000]
 */
fun echoServerMain(args: Array<String>) {
    val host = args.getOrNull(0) ?: "0.0.0.0"
    val port = args.getOrNull(1)?.toIntOrNull() ?: 9000
    println("echo-server listening on $host:$port")
    EventLoop.run {
        val server = listenTcp(host, port)
        while (true) {
            val conn = server.accept()
            launch { echoConnection(conn) }
        }
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
