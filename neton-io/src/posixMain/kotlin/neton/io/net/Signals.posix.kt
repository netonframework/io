@file:OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.alloc
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import neton.io.posixshim.neton_affinity_capture
import neton.io.posixshim.neton_pin_thread_nth
import neton.io.posixshim.neton_signal_install
import platform.posix.EINTR
import platform.posix.FD_CLOEXEC
import platform.posix.F_GETFL
import platform.posix.F_SETFD
import platform.posix.F_SETFL
import platform.posix.O_NONBLOCK
import platform.posix.SIGINT
import platform.posix.SIGQUIT
import platform.posix.SIGTERM
import platform.posix.errno
import platform.posix.fcntl
import platform.posix.pipe
import platform.posix.read
import platform.posix.usleep
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

private fun Signal.number(): Int = when (this) { Signal.Int -> SIGINT; Signal.Term -> SIGTERM; Signal.Quit -> SIGQUIT }

private object SignalPipe {
    private val state = AtomicInt(0)          // 0 none, 1 creating, 2 ready
    private val installed = AtomicInt(0)      // bit per Signal.ordinal
    var readFd = -1; private set
    var writeFd = -1; private set

    fun ensure() {
        if (state.load() == 2) return
        if (state.compareAndSet(0, 1)) {
            memScoped {
                val fds = allocArray<IntVar>(2)
                check(pipe(fds) == 0) { "pipe() failed (errno=$errno)" }
                readFd = fds[0]; writeFd = fds[1]
            }
            fcntl(readFd, F_SETFD, FD_CLOEXEC); fcntl(writeFd, F_SETFD, FD_CLOEXEC)
            // The handler must never block: a full pipe drops the byte (that signal is already pending).
            fcntl(writeFd, F_SETFL, fcntl(writeFd, F_GETFL) or O_NONBLOCK)
            state.store(2)
        } else {
            while (state.load() != 2) usleep(100u)
        }
    }

    fun install(s: Signal) {
        ensure()
        val bit = 1 shl s.ordinal
        while (true) {
            val cur = installed.load()
            if (cur and bit != 0) return
            if (installed.compareAndSet(cur, cur or bit)) break
        }
        check(neton_signal_install(s.number(), writeFd) == 0) { "sigaction(${s.name}) failed (errno=$errno)" }
    }
}

internal actual fun signalInstall(signal: Signal) = SignalPipe.install(signal)

internal actual fun signalWaitBlocking(): Signal? = memScoped {
    SignalPipe.ensure()
    val b = alloc<UByteVar>()
    val n = read(SignalPipe.readFd, b.ptr, 1u)
    if (n != 1L) { if (errno != EINTR) usleep(1000u); return@memScoped null }
    when (b.value.toInt()) { SIGINT -> Signal.Int; SIGTERM -> Signal.Term; SIGQUIT -> Signal.Quit; else -> null }
}

internal actual fun pinCurrentThread(n: Int): Int = neton_pin_thread_nth(n)

internal actual fun captureAffinity(): Int = neton_affinity_capture()
