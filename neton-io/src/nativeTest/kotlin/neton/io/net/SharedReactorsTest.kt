package neton.io.net

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.codec.LineCodec
import neton.io.core.Framed
import neton.io.core.Io
import neton.io.core.IoStream
import kotlin.concurrent.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** SPEC §27.5: several listening ports on one set of reactors; parallel name resolution. */
class SharedReactorsTest {
    private fun prefixEcho(prefix: String, threads: AtomicReference<Set<ULong>>): suspend (IoStream) -> Unit = { conn ->
        val t = currentThreadId()
        while (true) { val cur = threads.value; if (threads.compareAndSet(cur, cur + t)) break }
        try {
            val f = Framed(Io(conn), LineCodec, LineCodec)
            f.incoming().collect { f.send("$prefix$it") }
        } finally { conn.close() }
    }

    private suspend fun roundTrip(port: Int, line: String): String {
        val c = connect("127.0.0.1", port)
        try {
            val f = Framed(Io(c), LineCodec, LineCodec)
            f.send(line)
            return f.incoming().first()
        } finally { c.close() }
    }

    @Test
    fun twoPortsShareReactorsAndStopIndependently() = runReactor {
        val ta = AtomicReference<Set<ULong>>(emptySet()); val tb = AtomicReference<Set<ULong>>(emptySet())
        val a = listenGroup("127.0.0.1", 21960, reactors = 2)
        val b = a.listenAlso("127.0.0.1", 21961)
        assertEquals(2, b.reactors)
        val sa = launch { a.serve(prefixEcho("a:", ta)) }
        val sb = launch { b.serve(prefixEcho("b:", tb)) }
        val res = (1..16).map { i -> async { roundTrip(if (i % 2 == 0) 21960 else 21961, "m$i") } }.awaitAll()
        for ((k, r) in res.withIndex()) { val i = k + 1; assertEquals((if (i % 2 == 0) "a:" else "b:") + "m$i", r) }
        assertTrue(tb.value.size >= 2, "the second port must use both reactors, saw ${tb.value.size} thread(s)")
        a.shutdown(1_000); sa.join()
        assertEquals("b:still", withTimeout(2_000) { roundTrip(21961, "still") })   // b keeps its reactors
        b.shutdown(1_000); sb.join()
        withTimeout(5_000) { b.awaitWorkers() }                                     // now the workers exit
        assertFailsWith<IllegalStateException> { a.listenAlso("127.0.0.1", 21962) }
    }

    @Test
    fun namesResolveInParallel() = runReactor {
        val all = (1..32).map { async { resolve("localhost", 80, passive = false) } }.awaitAll()
        assertTrue(all.all { it.isNotEmpty() }, "every lookup must return addresses")
    }
}
