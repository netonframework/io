package neton.io.net

import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Runnable
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * An optional fast path (SPEC §24.11, contract §28.3): resumes a coroutine parked on a reactor
 * without a dispatch object, for hand-offs between coroutines of one reactor (a read loop handing
 * requests to a handler loop). Plain `resume` gives the same result with one more dispatch; a
 * protocol does not need this class.
 *
 * - The continuation must be a raw one (from `suspendCoroutineUninterceptedOrReturn`) of a
 *   coroutine dispatched by this reactor.
 * - Callable from any thread. On the reactor's thread the resume is queued (a ring slot), never run
 *   at the call site; from another thread it goes through the reactor's external queue.
 * - Returns true when accepted: the continuation will be resumed exactly once and the value is
 *   handed over. Returns false when refused, only once the reactor is closing or stopped: the
 *   continuation was not resumed and the value was not taken, so cleaning up stays with the caller.
 *   Since a reactor only closes after all coroutines in its scope have ended, false means the caller
 *   parked a coroutine outside that scope: a lifecycle bug to report.
 * - Exactly-once across racing paths (a normal resume and a cancellation) is the caller's job: keep
 *   the continuation in a slot, and let each path take it out of the slot on the reactor thread
 *   before resuming; cancellation handlers run on any thread and must post to the reactor first.
 */
class ReactorResumer internal constructor(private val reactor: Reactor) {
    fun <T> resume(cont: Continuation<T>, value: T): Boolean {
        if (reactor.isOwnerThread()) {
            if (!reactor.acceptsLocalWork()) return false
            reactor.enqueueResumeAny(cont, value, null)
            return true
        }
        return reactor.tryDispatchExternal(Runnable { cont.resume(value) })
    }

    fun <T> resumeWithException(cont: Continuation<T>, error: Throwable): Boolean {
        if (reactor.isOwnerThread()) {
            if (!reactor.acceptsLocalWork()) return false
            reactor.enqueueResumeAny(cont, null, error)
            return true
        }
        return reactor.tryDispatchExternal(Runnable { cont.resumeWithException(error) })
    }
}

/** The [ReactorResumer] for coroutines dispatched by [context]'s reactor, or null if it has none. */
fun reactorResumer(context: CoroutineContext): ReactorResumer? =
    (context[ContinuationInterceptor] as? Reactor)?.let { ReactorResumer(it) }
