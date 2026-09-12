package neton.io.net

import neton.io.bytes.Buffer
import neton.io.core.IoStream

/**
 * [IoStream] over a non-blocking TCP fd, driven by an [EventLoop].
 *
 * This is the P1 readiness driver: read/write issue the syscall directly and, on
 * EAGAIN, park on the reactor until the fd is ready again — one syscall per readiness,
 * which for kqueue/epoll is already optimal. The completion-oriented buffer-feed SPI
 * is added later for io_uring/IOCP (see SPEC).
 */
internal class SocketStream(
    private val fd: Int,
    private val loop: EventLoop,
    readBufferSize: Int = 64 * 1024,
) : IoStream {

    private val scratch = ByteArray(readBufferSize)

    override suspend fun read(dst: Buffer): Int {
        while (true) {
            val outcome = readOnce(fd, scratch)
            when (outcome.result) {
                IoResult.OK -> {
                    dst.writeBytes(scratch, 0, outcome.count)
                    return outcome.count
                }
                IoResult.EOF -> return -1
                IoResult.ERROR -> return -1
                IoResult.WOULD_BLOCK -> loop.waitReadable(fd)
            }
        }
    }

    override suspend fun write(src: Buffer): Int {
        val bytes = src.readAll()
        var offset = 0
        while (offset < bytes.size) {
            val n = writeOnce(fd, bytes, offset)
            when {
                n >= 0 -> offset += n
                n == WOULD_BLOCK -> loop.waitWritable(fd)
                else -> break // IO_ERROR
            }
        }
        return offset
    }

    override suspend fun flush() {}

    override fun close() {
        closeFd(fd)
    }
}
