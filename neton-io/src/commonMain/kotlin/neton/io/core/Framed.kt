package neton.io.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import neton.io.codec.Decoder
import neton.io.codec.Encoder

/**
 * Sends and receives codec-defined frames over an [Io].
 *
 * The read side [incoming] is a `Flow`: it decodes as much as the read buffer allows, then
 * reads more bytes when it cannot, ending at EOF. The write side [send] encodes into the
 * write buffer and flushes it downstream.
 */
class Framed<In, Out>(
    private val io: Io,
    private val decoder: Decoder<In>,
    private val encoder: Encoder<Out>,
) {
    /** Frame stream: read then decode; read more when a frame is incomplete; end at EOF. */
    fun incoming(): Flow<In> = flow {
        val buf = io.readBuf
        while (true) {
            var item = decoder.decode(buf)
            while (item != null) {
                emit(item)
                item = decoder.decode(buf)
            }
            buf.discardReadBytes()
            val n = io.stream.read(buf)
            if (n < 0) break // EOF
        }
    }

    /** Send one frame: encode, write, flush. */
    suspend fun send(item: Out) {
        val out = io.writeBuf
        encoder.encode(item, out)
        io.stream.write(out)
        io.stream.flush()
        out.clear()
    }
}
