package neton.io.net

/** The VM owns SIGINT/SIGTERM/SIGQUIT (shutdown hooks, thread dumps); a library must not take them. */
internal actual suspend fun TcpServerGroup.stopOnShutdownSignal(gracefulTimeoutMillis: Long, onSignal: () -> Unit) {
    throw UnsupportedOperationException(
        "serveTcp(shutdownOnSignals = true) is native-only: on the JVM, stop the server from a shutdown hook " +
            "by completing `until`",
    )
}
