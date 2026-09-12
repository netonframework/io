package neton.io.net

import neton.io.bytes.Buffer
import neton.io.core.IoStream

/**
 * [IoStream] over a non-blocking TCP fd, driven by an [EventLoop].
 *
 * Read and write go straight through the [Buffer] backing memory (no per-call allocation),
 * issuing the syscall directly and parking on the reactor on EAGAIN — one syscall per
 * readiness, which is already optimal for kqueue/epoll. The completion-oriented buffer-feed
 * path is added with io_uring/IOCP (see SPEC).
 */
internal class SocketStream(
    private val fd: Int,
    private val loop: EventLoop,
    private val readChunk: Int = 64 * 1024,
) : IoStream {

    override suspend fun read(dst: Buffer): Int {
        while (true) {
            val outcome = readInto(fd, dst, readChunk)
            when (outcome.result) {
                IoResult.OK -> return outcome.count
                IoResult.EOF -> return -1
                IoResult.ERROR -> return -1
                IoResult.WOULD_BLOCK -> loop.waitReadable(fd)
            }
        }
    }

    override suspend fun write(src: Buffer): Int {
        var total = 0
        while (src.readableBytes > 0) {
            val n = writeFrom(fd, src)
            when {
                n >= 0 -> total += n
                n == WOULD_BLOCK -> loop.waitWritable(fd)
                else -> break // IO_ERROR
            }
        }
        return total
    }

    override suspend fun flush() {}

    override fun close() {
        closeFd(fd)
    }
}
