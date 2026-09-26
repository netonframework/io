package neton.io.net

/**
 * Android reactor selection (SPEC §20): epoll by default (the Linux kernel; shared with Linux in
 * epollMain), poll(2) on request via NETON_IO_DRIVER=polling. io_uring is not offered: the app
 * sandbox's seccomp policy may block it, and there is no measurement yet to justify a probe.
 */
internal actual fun createReactor(): Reactor = when (driverSelection()) {
    "polling", "poll" -> ReadinessReactor(PollPoller())
    else -> ReadinessReactor(EpollPoller())
}
