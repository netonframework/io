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

private class SignalWaiter(val wanted: Set<Signal>, val result: CompletableDeferred<Signal>)

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
                for (w in locked { waiters }) if (s in w.wanted) w.result.complete(s)
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
    val w = SignalWaiter(signals.toSet(), CompletableDeferred())
    SignalHub.register(w)
    try {
        SignalHub.startWatcher()
        return w.result.await()
    } finally {
        SignalHub.unregister(w)
    }
}

/**
 * Keep [signals] handled for the whole of [block], whatever waiters come and go inside it, so there
 * is no gap in which one falls back to its previous action (SPEC §27.7: a second Ctrl-C during a
 * graceful stop must force it, not end the process by default action).
 */
internal suspend fun <T> holdingSignals(vararg signals: Signal, block: suspend () -> T): T {
    val hold = SignalWaiter(signals.toSet(), CompletableDeferred())
    SignalHub.register(hold)
    try { return block() } finally { SignalHub.unregister(hold) }
}

/** Waiters currently registered (tests: a signal sent before the waiter exists is dropped). */
internal fun signalWaiterCount(): Int = SignalHub.count()

/** Pin the calling thread to the [n]-th CPU (modulo) of the captured set; the CPU, or -1 (SPEC §27.2). */
internal expect fun pinCurrentThread(n: Int): Int

/** Capture the process's allowed CPU set before any thread is pinned; its size, or -1 where unsupported. */
internal expect fun captureAffinity(): Int
