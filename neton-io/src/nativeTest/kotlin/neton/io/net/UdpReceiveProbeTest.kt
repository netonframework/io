@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.toKString
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.getenv
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * A measurement, not a check (runs only with `NETON_IO_UDP_PROBE=1`): what one datagram costs to send and to receive
 * on this platform's UDP path, and how long a parked receive takes to wake up. Phases:
 * - send: [BURST] datagrams of [SIZE] bytes sent back to back, one per call and with segmentation offload (10 segments
 *   per call) where the platform has it;
 * - receive: the same burst drained with every datagram already queued, so no receive parks: the time per datagram
 *   and per call is the receive path alone;
 * - wake-up: one datagram sent to a socket whose receive is parked, [PINGS] times: the time from the send to the
 *   receive returning (reactor readiness or completion latency on one thread).
 */
class UdpReceiveProbeTest {
    private val enabled = getenv("NETON_IO_UDP_PROBE")?.toKString() == "1"

    @Test
    fun probe() {
        if (!enabled) return
        runReactor {
            val send = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT, UdpOptions(sendBufferSize = 4 shl 20))
            val recv = bindUdp(SocketAddress.IPV4_LOCALHOST_ANY_PORT, UdpOptions(receiveBufferSize = 8 shl 20))
            val driver = currentReactor().toString().substringBefore('@')
            println("probe: driver $driver, batch $BATCH_SIZE, gso ${send.maxGsoSegments}, gro ${recv.groSegments}, rcvbuf ${recv.receiveBufferSize()}")
            for (segments in listOf(1, minOf(10, send.maxGsoSegments)).distinct()) {
                repeat(3) { round -> burst(send, recv, segments, round) }
            }
            wakeUps(send, recv)
            send.close()
            recv.close()
        }
    }

    private suspend fun burst(send: UdpSocket, recv: UdpSocket, segments: Int, round: Int) {
        Transmit(SIZE * segments).use { t ->
            t.length = SIZE * segments
            t.segmentSize = if (segments > 1) SIZE else 0
            t.setDestination(recv.localAddress)
            // Bursts of [CHUNK] datagrams, each drained before the next: Linux caps the receive buffer (rmem_max,
            // about 200 KiB on GitHub's runners), and a larger burst would be partly dropped
            val callsPerChunk = CHUNK / segments
            var sendNs = 0L
            var recvNs = 0L
            var recvCalls = 0
            var datagrams = 0
            RecvBatch(BATCH_SIZE, 64 * 1024).use { b ->
                repeat(BURST / CHUNK) {
                    val sendStart = TimeSource.Monotonic.markNow()
                    repeat(callsPerChunk) { send.send(t) }
                    sendNs += sendStart.elapsedNow().inWholeNanoseconds
                    var got = 0
                    val recvStart = TimeSource.Monotonic.markNow()
                    while (got < callsPerChunk * segments) {
                        val n = withTimeoutOrNull(2.seconds) { recv.recv(b) } ?: error("datagrams lost: $got of ${callsPerChunk * segments}")
                        recvCalls++
                        for (i in 0 until n) got += b.length(i) / b.stride(i)
                    }
                    recvNs += recvStart.elapsedNow().inWholeNanoseconds
                    datagrams += got
                }
            }
            val calls = BURST / segments
            assertEquals(BURST, datagrams)
            println(
                "probe: segments $segments round $round: send ${sendNs / 1000} us (${sendNs / BURST} ns/datagram, " +
                    "${sendNs / calls} ns/call); receive ${recvNs / 1000} us (${recvNs / datagrams} ns/datagram, " +
                    "$recvCalls calls, ${recvNs / recvCalls} ns/call)",
            )
        }
    }

    private suspend fun kotlinx.coroutines.CoroutineScope.wakeUps(send: UdpSocket, recv: UdpSocket) {
        val samples = LongArray(PINGS)
        Transmit(SIZE).use { t ->
            t.length = SIZE
            t.setDestination(recv.localAddress)
            RecvBatch(BATCH_SIZE, 64 * 1024).use { b ->
                for (i in 0 until PINGS) {
                    var sentAt = TimeSource.Monotonic.markNow()
                    val receiver = launch {
                        withTimeoutOrNull(2.seconds) { recv.recv(b) } ?: error("ping $i lost")
                        samples[i] = sentAt.elapsedNow().inWholeNanoseconds
                    }
                    kotlinx.coroutines.yield() // the receive parks
                    sentAt = TimeSource.Monotonic.markNow()
                    send.send(t)
                    receiver.join()
                }
            }
        }
        samples.sort()
        println(
            "probe: wake-up over $PINGS: p50 ${samples[PINGS / 2] / 1000} us, p90 ${samples[PINGS * 9 / 10] / 1000} us, " +
                "max ${samples.last() / 1000} us",
        )
    }

    private companion object {
        const val SIZE = 1200
        const val BURST = 2000
        const val CHUNK = 100
        const val PINGS = 200
    }
}
