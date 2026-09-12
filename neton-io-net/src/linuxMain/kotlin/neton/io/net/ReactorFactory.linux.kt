package neton.io.net

/**
 * Linux: io_uring (completion) when NETON_IO_DRIVER=iouring, otherwise the readiness reactor
 * over epoll (default) or poll.
 */
internal actual fun createReactor(): Reactor = when (driverSelection()) {
    "iouring", "uring" -> UringReactor()
    else -> ReadinessReactor(createPoller())
}
