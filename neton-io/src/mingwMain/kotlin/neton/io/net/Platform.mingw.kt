@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import platform.windows.GetCurrentThreadId
import platform.windows.GetSystemInfo
import platform.windows.SYSTEM_INFO

/** Windows reactor (SPEC §20): WSAPoll readiness for now; IOCP (completion) is the performance driver to come. */
internal actual fun createReactor(): Reactor = ReadinessReactor(WsaPollPoller())

internal actual fun createPoller(): Poller = WsaPollPoller()

internal actual fun currentThreadId(): ULong = GetCurrentThreadId().toULong()

actual fun cpuCount(): Int = memScoped {
    val info = alloc<SYSTEM_INFO>()
    GetSystemInfo(info.ptr)
    info.dwNumberOfProcessors.toInt().coerceAtLeast(1)
}
