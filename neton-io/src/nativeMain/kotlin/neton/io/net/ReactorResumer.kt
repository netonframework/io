package neton.io.net

import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.intrinsics.intercepted
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Resumes coroutines parked on a reactor from that reactor's own thread without a dispatch object
 * (SPEC §24.11): for queues between coroutines of one connection (a read loop handing requests to a
 * handler loop). Obtain it once with [reactorResumer]; [resume] then costs a ring slot.
 *
 * The continuation must be a raw one (from `suspendCoroutineUninterceptedOrReturn`) of a coroutine
 * running on this reactor. Called from any other thread, it falls back to the dispatched resume.
 */
class ReactorResumer internal constructor(private val reactor: Reactor) {
    fun <T> resume(cont: Continuation<T>, value: T) {
        if (reactor.isOwnerThread()) reactor.enqueueResumeAny(cont, value, null)
        else cont.intercepted().resume(value)
    }

    fun <T> resumeWithException(cont: Continuation<T>, error: Throwable) {
        if (reactor.isOwnerThread()) reactor.enqueueResumeAny(cont, null, error)
        else cont.intercepted().resumeWithException(error)
    }
}

/** The [ReactorResumer] for coroutines dispatched by [context]'s reactor, or null if it has none. */
fun reactorResumer(context: CoroutineContext): ReactorResumer? =
    (context[ContinuationInterceptor] as? Reactor)?.let { ReactorResumer(it) }
