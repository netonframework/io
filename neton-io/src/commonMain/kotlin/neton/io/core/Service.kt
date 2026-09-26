package neton.io.core

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
