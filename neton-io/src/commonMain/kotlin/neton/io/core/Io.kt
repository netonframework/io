package neton.io.core

import neton.io.bytes.Buffer

/**
 * Holds a (possibly filtered) byte stream plus its read and write buffers.
 *
 * The upper layer ([Framed]) decodes from [readBuf] and encodes into [writeBuf]; [stream]
 * performs the actual transfer, which may be a raw driver stream or a stack of [Filter]s.
 */
class Io(val stream: IoStream) {
    val readBuf: Buffer = Buffer()
    val writeBuf: Buffer = Buffer()

    fun close() = stream.close()

    companion object {
        /**
         * Stack filter layers over a raw stream, innermost first.
         * Example: `Io.of(sock, ::BaseFilter, ::TlsFilter)` yields TLS(Base(sock)).
         */
        fun of(raw: IoStream, vararg layers: (IoStream) -> Filter): Io {
            var s: IoStream = raw
            for (layer in layers) s = layer(s)
            return Io(s)
        }
    }
}
