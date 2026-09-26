@file:OptIn(ExperimentalForeignApi::class)

package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import platform.windows.GetCurrentThreadId
import platform.windows.GetSystemInfo
import platform.windows.SYSTEM_INFO

/**
 * Windows reactor (SPEC §23.1): IOCP completion by default; NETON_IO_DRIVER=wsapoll (or poll /
 * polling) selects the WSAPoll readiness driver.
 */
internal actual fun createReactor(): Reactor = when (driverSelection()) {
    "wsapoll", "poll", "polling" -> ReadinessReactor(WsaPollPoller())
    else -> IocpReactor()
}

internal actual fun createPoller(): Poller = WsaPollPoller()

internal actual fun currentThreadId(): ULong = GetCurrentThreadId().toULong()

actual fun cpuCount(): Int = memScoped {
    val info = alloc<SYSTEM_INFO>()
    GetSystemInfo(info.ptr)
    info.dwNumberOfProcessors.toInt().coerceAtLeast(1)
}

/** Windows has no SO_REUSEPORT. */
internal actual val reusePortBalancesLoad: Boolean = false
