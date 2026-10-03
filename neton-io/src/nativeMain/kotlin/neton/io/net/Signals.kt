@file:OptIn(ExperimentalAtomicApi::class, kotlin.native.concurrent.ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.native.concurrent.Worker

/**
 * Process signals a server reacts to (SPEC §27.1). On Windows: Ctrl-C is [Int], Ctrl-Break is
 * [Quit], and closing the console, logoff or shutdown is [Term].
 */
enum class Signal { Int, Term, Quit }

/** Install our handler for [signal], saving the process's previous action (first waiter; SPEC §27.7). */
internal expect fun signalInstall(signal: Signal)

/** Put back the action [signal] had before [signalInstall] (last waiter gone; SPEC §27.7). */
internal expect fun signalRestore(signal: Signal)

/** Block the calling thread until a signal arrives; null for one this process does not map. */
internal expect fun signalWaitBlocking(): Signal?

/** A registration: [deliver] is called (on the watcher thread) for every wanted signal that arrives. */
private class SignalWaiter(val wanted: Set<Signal>, val deliver: (Signal) -> Unit)

/**
 * Waiters and, per signal, how many of them want it. Our handler is installed for a signal only
 * while that count is above zero; the previous action is restored when it drops to zero, so the
 * process's signal behaviour is changed only while someone is waiting (SPEC §27.7). Registration
 * and delivery may come from any thread: a short lock guards the list and the counts, and the
 * install / restore calls happen under it, in order.
 */
private object SignalHub {
    private val lock = AtomicInt(0)
    private var waiters: List<SignalWaiter> = emptyList()
    private val counts = IntArray(Signal.entries.size)
    private val started = AtomicInt(0)

    private inline fun <T> locked(block: () -> T): T {
        while (!lock.compareAndSet(0, 1)) { }
        try { return block() } finally { lock.store(0) }
    }

    fun register(w: SignalWaiter) = locked {
        val done = ArrayList<Signal>()
        try {
            for (s in w.wanted) {
                if (counts[s.ordinal] == 0) signalInstall(s)
                counts[s.ordinal]++
                done.add(s)
            }
        } catch (t: Throwable) {
            for (s in done) if (--counts[s.ordinal] == 0) signalRestore(s)
            throw t
        }
        waiters = waiters + w
    }

    fun unregister(w: SignalWaiter) = locked {
        if (w !in waiters) return@locked
        waiters = waiters - w
        for (s in w.wanted) if (--counts[s.ordinal] == 0) signalRestore(s)
    }

    fun count(): Int = locked { waiters.size }

    /** One thread blocks on the platform's signal source and completes the waiters that want it. */
    fun startWatcher() {
        if (!started.compareAndSet(0, 1)) return
        Worker.start(name = "neton-signals").executeAfter(0L) {
            while (true) {
                val s = signalWaitBlocking() ?: continue
                for (w in locked { waiters }) if (s in w.wanted) w.deliver(s)
            }
        }
    }
}

/**
 * Suspend until the process receives one of [signals]; returns which (SPEC §27.1, §27.7). While at
 * least one caller waits for a signal, that signal no longer has its previous effect (such as ending
 * the process); when the last waiter returns or is cancelled, the previous action is restored.
 * Resumes on the caller's dispatcher; cancellable.
 */
suspend fun awaitSignal(vararg signals: Signal = arrayOf(Signal.Int, Signal.Term, Signal.Quit)): Signal {
    require(signals.isNotEmpty()) { "no signals to wait for" }
    val result = CompletableDeferred<Signal>()
    val w = SignalWaiter(signals.toSet()) { result.complete(it) }
    SignalHub.register(w)
    try {
        SignalHub.startWatcher()
        return result.await()
    } finally {
        SignalHub.unregister(w)
    }
}

/**
 * A lasting subscription to [signals]: every one that arrives is queued until [receive]d, so a
 * sequence of signals (a stop, then a forced stop) cannot lose one between two waits (SPEC §27.8).
 * The signals are handled from creation until [close]; then their previous actions are restored.
 */
internal class SignalSubscription(signals: Set<Signal>) {
    private val queue = kotlinx.coroutines.channels.Channel<Signal>(kotlinx.coroutines.channels.Channel.UNLIMITED)
    private val w = SignalWaiter(signals) { queue.trySend(it) }
    private var closed = false

    init { SignalHub.register(w); SignalHub.startWatcher() }

    suspend fun receive(): Signal = queue.receive()

    fun close() { if (!closed) { closed = true; SignalHub.unregister(w); queue.close() } }
}

/** Run [block] with a [SignalSubscription] to [signals], closed afterwards whatever happens. */
internal suspend fun <T> withSignals(vararg signals: Signal, block: suspend (SignalSubscription) -> T): T {
    val sub = SignalSubscription(signals.toSet())
    try { return block(sub) } finally { sub.close() }
}

/** Waiters currently registered (tests: a signal sent before the waiter exists is dropped). */
internal fun signalWaiterCount(): Int = SignalHub.count()

