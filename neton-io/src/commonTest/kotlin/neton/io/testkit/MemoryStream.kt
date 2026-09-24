package neton.io.testkit

import kotlinx.coroutines.channels.Channel
import neton.io.bytes.Buffer
import neton.io.core.IoStream

/**
 * In-memory [IoStream], one end of a socketpair-like duplex, with no platform I/O.
 *
 * This is the P0 "driver": it stands in for kqueue/epoll so the whole
 * bytes/codec/io/filter/framed/dispatcher model can be built and tested in pure
 * Kotlin/Native. The network drivers replace this class later; [IoStream] is unchanged.
 *
 * Suspension is provided by the channel: a read with nothing pending suspends and resumes
 * when the peer sends; a closed peer surfaces as EOF.
 */
private class MemoryStream(
    private val inbound: Channel<ByteArray>,
    private val outbound: Channel<ByteArray>,
) : IoStream {

    override suspend fun read(dst: Buffer): Int {
        val bytes = inbound.receiveCatching().getOrNull() ?: return -1 // closed and drained = EOF
        dst.writeBytes(bytes)
        return bytes.size
    }

    override suspend fun write(src: Buffer): Int {
        val n = src.readableBytes
        if (n == 0) return 0
        outbound.send(src.readBytes(n))
        return n
    }

    override suspend fun flush() {}

    override fun close() {
        outbound.close()
    }
}

/** Duplex pair `(client, server)`: bytes written on one end are readable on the other. */
fun memoryPair(): Pair<IoStream, IoStream> {
    val a = Channel<ByteArray>(Channel.UNLIMITED) // client -> server
    val b = Channel<ByteArray>(Channel.UNLIMITED) // server -> client
    val client = MemoryStream(inbound = b, outbound = a)
    val server = MemoryStream(inbound = a, outbound = b)
    return client to server
}
