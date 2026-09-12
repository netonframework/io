package neton.io.net

/** Apple: readiness reactor over kqueue (or poll via NETON_IO_DRIVER=polling). */
internal actual fun createReactor(): Reactor = ReadinessReactor(createPoller())
