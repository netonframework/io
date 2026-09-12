package neton.io.codec

import neton.io.bytes.Buffer

/**
 * Decodes one frame from [buf]. Returns `null` when there is not enough data yet ("need
 * more bytes"); in that case it must not consume bytes that cannot form a complete frame.
 */
fun interface Decoder<out T> {
    fun decode(buf: Buffer): T?
}

/** Encodes [item] into [out]. */
fun interface Encoder<in T> {
    fun encode(item: T, out: Buffer)
}

/**
 * Newline-delimited text codec (`\n` terminated). Decoding excludes the trailing newline;
 * encoding appends one.
 */
object LineCodec : Decoder<String>, Encoder<String> {
    private const val NL: Byte = '\n'.code.toByte()

    override fun decode(buf: Buffer): String? {
        val nl = buf.indexOf(NL)
        if (nl < 0) return null
        val line = buf.readBytes(nl) // excludes \n
        buf.skip(1)                  // drop \n
        return line.decodeToString()
    }

    override fun encode(item: String, out: Buffer) {
        out.writeBytes(item.encodeToByteArray())
        out.writeByte(NL)
    }
}
