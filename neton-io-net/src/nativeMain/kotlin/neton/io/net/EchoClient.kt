package neton.io.net

import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import kotlin.time.TimeSource

/**
 * Closed-loop echo load generator: opens N connections, each doing request/response
 * ping-pong for the duration, then reports aggregate throughput.
 *
 * Usage: echoClient [host=127.0.0.1] [port=9000] [connections=50] [seconds=5] [payload=64]
 */
fun echoClientMain(args: Array<String>) {
    val host = args.getOrNull(0) ?: "127.0.0.1"
    val port = args.getOrNull(1)?.toIntOrNull() ?: 9000
    val connections = args.getOrNull(2)?.toIntOrNull() ?: 50
    val seconds = args.getOrNull(3)?.toIntOrNull() ?: 5
    val payloadSize = args.getOrNull(4)?.toIntOrNull() ?: 64

    val payload = ByteArray(payloadSize) { 'x'.code.toByte() }
    val deadlineMs = seconds * 1000L
    val clock = TimeSource.Monotonic.markNow()
    var totalRequests = 0L

    runReactor {
        val jobs = ArrayList<Job>(connections)
        repeat(connections) {
            jobs.add(launch {
                val conn = connect(host, port)
                val writeBuf = Buffer(payloadSize)
                val readBuf = Buffer(payloadSize)
                var requests = 0L
                while (clock.elapsedNow().inWholeMilliseconds < deadlineMs) {
                    writeBuf.clear()
                    writeBuf.writeBytes(payload)
                    conn.write(writeBuf)

                    readBuf.clear()
                    var received = 0
                    while (received < payloadSize) {
                        val n = conn.read(readBuf)
                        if (n < 0) break
                        received += n
                    }
                    if (received < payloadSize) break
                    requests++
                }
                conn.close()
                totalRequests += requests
            })
        }
        jobs.forEach { it.join() }
    }

    val elapsedMs = clock.elapsedNow().inWholeMilliseconds
    val elapsedSec = elapsedMs / 1000.0
    val qps = if (elapsedSec > 0) (totalRequests / elapsedSec).toLong() else 0
    val mbps = qps * payloadSize * 2.0 / (1024 * 1024) // request + echoed response
    println("connections=$connections duration=${elapsedSec}s payload=${payloadSize}B")
    println("total_requests=$totalRequests")
    println("throughput=$qps req/s")
    println("data_rate=${(mbps).toLong()} MiB/s (both directions)")
}
