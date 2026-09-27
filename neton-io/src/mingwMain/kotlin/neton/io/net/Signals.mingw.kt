@file:OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import neton.io.win.neton_ctrl_install
import neton.io.win.neton_ctrl_wait
import neton.io.win.neton_pin_thread_nth
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

private val ctrlInstalled = AtomicInt(0)

/** One console control handler covers every [Signal] (SPEC §27.1). */
internal actual fun signalInstall(signal: Signal) {
    if (ctrlInstalled.compareAndSet(0, 1)) check(neton_ctrl_install() == 0) { "SetConsoleCtrlHandler failed" }
}

internal actual fun signalWaitBlocking(): Signal? = when (neton_ctrl_wait()) {
    1 -> Signal.Int; 2 -> Signal.Term; 3 -> Signal.Quit; else -> null
}

internal actual fun pinCurrentThread(n: Int): Int = neton_pin_thread_nth(n)

/** Windows reads the process mask at pin time; threads inherit nothing that would narrow it. */
internal actual fun captureAffinity(): Int = cpuCount()
