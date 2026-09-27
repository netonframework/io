package neton.io.core

import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/**
 * Handles one request and produces one response, coroutine-native. Backpressure via a
 * readiness gate is added later; P0 keeps the minimal shape.
 */
fun interface Service<in Req, out Res> {
    suspend fun call(req: Req): Res
}

/**
 * Generic framed connection loop: read In, call [Service], write Out, until EOF.
 *
 * The loop runs on its owning connection's reactor dispatcher, preserving connection
 * affinity. A heavy handler that needs offloading does so explicitly inside [service];
 * this loop does not hop threads per frame.
 */
suspend fun <In, Out> serve(framed: Framed<In, Out>, service: Service<In, Out>) {
    // SPEC §23.2: answers every request already received, then flushes once (pipelining).
    framed.serveLoop { req -> service.call(req) }
}

/**
 * A [Service] allowing at most [max] calls at a time (SPEC §27.3, geario's `InFlight`). The limit is
 * shared by every connection that uses this instance; a call beyond it suspends until one finishes,
 * and since [serve] handles a connection's requests in order, that connection stops reading
 * meanwhile: TCP backpressure reaches the client. Each call adds one coroutine frame.
 */
class InFlightService<in Req, out Res> internal constructor(private val inner: Service<Req, Res>, val max: Int) : Service<Req, Res> {
    private val permits = Semaphore(max)

    /** Calls running now. */
    val inFlight: Int get() = max - permits.availablePermits

    override suspend fun call(req: Req): Res = permits.withPermit { inner.call(req) }
}

/** Limit this service to [max] concurrent calls (SPEC §27.3). */
fun <Req, Res> Service<Req, Res>.limitInFlight(max: Int): InFlightService<Req, Res> {
    require(max > 0) { "max must be positive" }
    return InFlightService(this, max)
}
