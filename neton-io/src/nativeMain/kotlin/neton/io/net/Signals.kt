@file:OptIn(ExperimentalAtomicApi::class, kotlin.native.concurrent.ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.native.concurrent.Worker

/**
 * Process signals a server reacts to (SPEC §27.1). On Windows: Ctrl-C is [Int], Ctrl-Break is
 * [Quit], and closing the console, logoff or shutdown is [Term].
 */
enum class Signal { Int, Term, Quit }

/** Install the handler for [signal] (idempotent). Called before a waiter can miss it. */
internal expect fun signalInstall(signal: Signal)

/** Block the calling thread until a signal arrives; null for one this process does not map. */
internal expect fun signalWaitBlocking(): Signal?

private class SignalWaiter(val wanted: Set<Signal>, val result: CompletableDeferred<Signal>)

private object SignalHub {
    val waiters = AtomicReference<List<SignalWaiter>>(emptyList())
    private val started = AtomicInt(0)

    fun add(w: SignalWaiter) { while (true) { val cur = waiters.load(); if (waiters.compareAndSet(cur, cur + w)) return } }
    fun remove(w: SignalWaiter) { while (true) { val cur = waiters.load(); if (waiters.compareAndSet(cur, cur - w)) return } }

    /** One thread blocks on the platform's signal source and completes the waiters that want it. */
    fun startWatcher() {
        if (!started.compareAndSet(0, 1)) return
        Worker.start(name = "neton-signals").executeAfter(0L) {
            while (true) {
                val s = signalWaitBlocking() ?: continue
                for (w in waiters.load()) if (s in w.wanted) w.result.complete(s)
            }
        }
    }
}

/**
 * Suspend until the process receives one of [signals]; returns which (SPEC §27.1). The handler for
 * a signal is installed the first time it is awaited and stays installed: from then on that signal
 * no longer has its default effect (such as ending the process), even while nobody waits for it.
 * Resumes on the caller's dispatcher; cancellable.
 */
suspend fun awaitSignal(vararg signals: Signal = arrayOf(Signal.Int, Signal.Term, Signal.Quit)): Signal {
    require(signals.isNotEmpty()) { "no signals to wait for" }
    val w = SignalWaiter(signals.toSet(), CompletableDeferred())
    SignalHub.add(w)
    try {
        for (s in signals) signalInstall(s)
        SignalHub.startWatcher()
        return w.result.await()
    } finally {
        SignalHub.remove(w)
    }
}

/** Waiters currently registered (tests: a signal sent before the waiter exists is dropped). */
internal fun signalWaiterCount(): Int = SignalHub.waiters.load().size

/** Pin the calling thread to the [n]-th CPU (modulo) of the captured set; the CPU, or -1 (SPEC §27.2). */
internal expect fun pinCurrentThread(n: Int): Int

/** Capture the process's allowed CPU set before any thread is pinned; its size, or -1 where unsupported. */
internal expect fun captureAffinity(): Int
