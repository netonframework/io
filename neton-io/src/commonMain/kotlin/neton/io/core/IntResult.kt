package neton.io.core

import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

@PublishedApi internal val intBoxes = arrayOfNulls<Any>(65537)

/**
 * [v] as a shared boxed Int for 0..65536 (SPEC §24), created once and reused, so resuming a
 * continuation with a byte count allocates nothing. Racy first stores are harmless: equal values.
 */
@PublishedApi internal fun boxedInt(v: Int): Any {
    if (v < 0 || v > 65536) return v
    return intBoxes[v] ?: (v as Any).also { intBoxes[v] = it }
}

/**
 * Return [n] from a suspend function without allocating (SPEC §24): `return intResult(n)`.
 *
 * A suspend function's result is an `Any?` at the ABI level, so `return n` boxes every Int outside
 * -128..127 — one heap object per read or write of more than 127 bytes. Returning through the intrinsic
 * hands a shared box up unchanged; the caller's state machine unboxes it. For [IoStream] implementations
 * and wrappers (TLS, protocol streams) on their read/write paths.
 */
@Suppress("NOTHING_TO_INLINE")
suspend inline fun intResult(n: Int): Int = suspendCoroutineUninterceptedOrReturn { boxedInt(n) }
