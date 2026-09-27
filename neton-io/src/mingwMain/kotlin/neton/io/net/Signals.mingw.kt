@file:OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import neton.io.win.neton_ctrl_install
import neton.io.win.neton_ctrl_uninstall
import neton.io.win.neton_ctrl_wait
import neton.io.win.neton_pin_thread_nth
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/** Signals wanted now; one console control handler covers them all (SPEC §27.1, §27.7). Hub lock held. */
private var ctrlUsers = 0

internal actual fun signalInstall(signal: Signal) {
    if (ctrlUsers == 0) check(neton_ctrl_install() == 0) { "SetConsoleCtrlHandler failed" }
    ctrlUsers++
}

internal actual fun signalRestore(signal: Signal) {
    if (--ctrlUsers == 0) neton_ctrl_uninstall()
}

internal actual fun signalWaitBlocking(): Signal? = when (neton_ctrl_wait()) {
    1 -> Signal.Int; 2 -> Signal.Term; 3 -> Signal.Quit; else -> null
}

internal actual fun pinCurrentThread(n: Int): Int = neton_pin_thread_nth(n)

/** Windows reads the process mask at pin time; threads inherit nothing that would narrow it. */
internal actual fun captureAffinity(): Int = cpuCount()
