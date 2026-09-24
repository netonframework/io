package neton.io.net

/**
 * Linux reactor selection.
 *
 * Default: io_uring, falling back to epoll when the kernel has no io_uring (setup fails). The
 * fallback is silent because it is a capability difference, not an error. NETON_IO_DRIVER pins
 * a specific driver: `epoll`, `polling`/`poll`, or `iouring`/`uring` (which errors if it cannot
 * be created rather than falling back).
 */
internal actual fun createReactor(): Reactor = when (driverSelection()) {
    "epoll" -> ReadinessReactor(EpollPoller())
    "polling", "poll" -> ReadinessReactor(PollPoller())
    "iouring", "uring" -> UringReactor() // explicit request: do not fall back
    else -> runCatching { UringReactor() }.getOrElse { ReadinessReactor(EpollPoller()) }
}
