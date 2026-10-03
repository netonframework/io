package neton.io.net

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

// The parts of the server API that use process signals, which neton-io handles only on native
// targets: on the JVM, signals belong to the virtual machine.

/**
 * Wait for a termination signal, then stop this server (SPEC §27.1, §27.7, as geario does):
 * [Signal.Int] stops at once (open connections are cancelled); [Signal.Term] and [Signal.Quit] stop
 * gracefully, letting connections finish for up to [gracefulTimeoutMillis], and any second one of
 * these signals meanwhile cancels the rest at once. The signals are handled only while this runs;
 * their previous actions are restored when it returns (or is cancelled). Call on reactor 0.
 */
suspend fun TcpServerGroup.shutdownOnSignal(gracefulTimeoutMillis: Long = 30_000): Signal =
    stopOnSignal(gracefulTimeoutMillis) {}

internal suspend fun TcpServerGroup.stopOnSignal(gracefulTimeoutMillis: Long, onSignal: () -> Unit): Signal =
    // One subscription for the whole stop: the second signal is queued even if it arrives before
    // the force wait starts (SPEC §27.8).
    withSignals(Signal.Int, Signal.Term, Signal.Quit) { sub ->
        val s = sub.receive()
        onSignal()
        if (s == Signal.Int) {
            shutdown(0)
        } else coroutineScope {
            val force = launch { sub.receive(); cancelConnections() }
            try { shutdown(gracefulTimeoutMillis) } finally { force.cancel() }
        }
        s
    }

internal actual suspend fun TcpServerGroup.stopOnShutdownSignal(gracefulTimeoutMillis: Long, onSignal: () -> Unit) {
    stopOnSignal(gracefulTimeoutMillis, onSignal)
}
