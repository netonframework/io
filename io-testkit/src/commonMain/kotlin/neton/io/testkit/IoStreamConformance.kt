@file:OptIn(kotlinx.coroutines.DelicateCoroutinesApi::class, kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package neton.io.testkit

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.core.ClosedException
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.io.core.StreamCapability
import neton.io.core.TimeoutException
import kotlin.random.Random

/** A connected pair: bytes written on [a] are read on [b] and the other way round. */
class StreamPair(val a: IoStream, val b: IoStream, private val cleanup: suspend () -> Unit = {}) {
    /** Close both ends (idempotent) and release whatever the factory set up. */
    suspend fun dispose() {
        runCatching { a.close() }; runCatching { b.close() }
        cleanup()
    }
}

/**
 * The [IoStream] conformance suite (neton-io SPEC §28.6). Give it a way to open a fresh connected
 * pair and, if the transport can do it, a pair whose [StreamPair.b] resets (RST) the connection when
 * closed. Every mandatory rule is checked; the optional ones follow the capabilities that [StreamPair.a]
 * declares: a declared capability must work, an undeclared one must be refused.
 *
 * Run it inside the coroutine context the streams need (for neton-io sockets: `runReactor { }`).
 * [run] returns the failures, one line each; an empty list means the stream conforms.
 */
class IoStreamConformance(
    private val name: String,
    private val open: suspend () -> StreamPair,
    private val openResetting: (suspend () -> StreamPair)? = null,
    /** Per-check time limit. */
    private val timeoutMillis: Long = 5_000,
    /**
     * How [StreamPair.a] ends its output in an orderly way before the peer checks for EOF. Plain
     * `close()` for byte streams; a stream whose `close()` cannot send its protocol's end marker (TLS:
     * close_notify needs a write, and `close()` does not suspend) passes `{ it.shutdownOutput(); it.close() }`.
     */
    private val orderlyClose: suspend (IoStream) -> Unit = { it.close() },
) {
    private val failures = ArrayList<String>()

    suspend fun run(): List<String> {
        failures.clear()
        check("read appends and never returns 0") { p -> readAppends(p) }
        check("write writes everything and empties src") { p -> writeAll(p) }
        check("src is reusable once write returns") { p -> srcReusable(p) }
        check("peer close (FIN): data, then -1") { p -> finEof(p) }
        check("write to a closed peer fails with IoException") { p -> writeToClosedPeer(p) }
        check("close is idempotent; parked and later calls get ClosedException") { p -> closeRules(p) }
        check("a second concurrent read is refused") { p -> concurrentRead(p) }
        check("a second concurrent write is refused") { p -> concurrentWrite(p) }
        check("writev writes in order") { p -> writevOrder(p) }
        check("any split of the bytes arrives intact") { p -> splitFuzz(p) }
        check("a cancelled write advances src by what was sent, no more, no less") { p -> cancelledWriteAccounting(p) }
        if (openResetting != null) checkWith("peer reset (RST) is an IoException, not EOF", openResetting) { p -> resetIsError(p) }
        check("HalfClose: declared works, undeclared is refused") { p -> halfClose(p) }
        check("ReadTimeout: declared works, undeclared is refused") { p -> readTimeout(p) }
        check("WriteTimeout / IdleTimeout: declared accepted, undeclared refused") { p -> writeIdleTimeouts(p) }
        check("ResumableAfterCancel: declared keeps the stream usable; undeclared closes it") { p -> afterCancel(p) }
        check("AnyThread: declared is usable from another thread") { p -> anyThread(p) }
        return failures.map { "$name: $it" }
    }

    private suspend fun check(what: String, body: suspend (StreamPair) -> Unit) = checkWith(what, open, body)

    private suspend fun checkWith(what: String, factory: suspend () -> StreamPair, body: suspend (StreamPair) -> Unit) {
        val pair = try { factory() } catch (t: Throwable) { failures.add("$what: could not open a pair: $t"); return }
        try {
            withTimeout(timeoutMillis) { body(pair) }
        } catch (t: Throwable) {
            failures.add("$what: $t")
        } finally {
            pair.dispose()
        }
    }

    private fun fail(msg: String): Nothing = throw AssertionError(msg)
    private fun expect(cond: Boolean, msg: () -> String) { if (!cond) fail(msg()) }

    private fun buf(bytes: ByteArray) = Buffer().also { it.writeBytes(bytes) }
    private fun buf(s: String) = buf(s.encodeToByteArray())

    /** Read from [s] until [n] bytes arrived (or EOF); returns them. */
    private suspend fun readExactly(s: IoStream, n: Int): ByteArray {
        val acc = Buffer()
        while (acc.readableBytes < n) {
            val r = Buffer()
            val k = s.read(r)
            expect(k != 0) { "read returned 0" }
            if (k < 0) break
            acc.writeBytes(r.readAll())
        }
        return acc.readAll()
    }

    /** Read [s] until EOF; returns everything. */
    private suspend fun readToEof(s: IoStream): ByteArray {
        val acc = Buffer()
        while (true) {
            val r = Buffer()
            val k = s.read(r)
            expect(k != 0) { "read returned 0" }
            if (k < 0) return acc.readAll()
            acc.writeBytes(r.readAll())
        }
    }

    private fun pattern(n: Int, seed: Int = 7): ByteArray { val r = Random(seed); return ByteArray(n) { r.nextInt().toByte() } }

    // ---- mandatory -------------------------------------------------------------------------

    private suspend fun readAppends(p: StreamPair) {
        p.b.write(buf("hello"))
        val dst = buf("x")
        val n = p.a.read(dst)
        expect(n > 0) { "read returned $n" }
        val got = dst.readAll().decodeToString()
        expect(got.startsWith("x") && got.length == 1 + n) { "dst must keep its bytes and gain exactly n: got '$got' for n=$n" }
    }

    private suspend fun writeAll(p: StreamPair) = coroutineScope {
        val data = pattern(256 * 1024)
        val reader = async { readExactly(p.b, data.size) }
        val src = buf(data)
        val n = p.a.write(src)
        expect(n == data.size) { "write returned $n for ${data.size}" }
        expect(src.readableBytes == 0) { "src still has ${src.readableBytes} bytes" }
        expect(reader.await().contentEquals(data)) { "the peer read different bytes" }
    }

    private suspend fun srcReusable(p: StreamPair) {
        val src = buf("abc")
        val array = src.backingArray()
        p.a.write(src)
        array.fill('z'.code.toByte())                       // reuse right after write returned
        expect(readExactly(p.b, 3).decodeToString() == "abc") { "the peer saw bytes written into src after write returned" }
    }

    private suspend fun finEof(p: StreamPair) {
        p.a.write(buf("tail"))
        orderlyClose(p.a)
        expect(readToEof(p.b).decodeToString() == "tail") { "the data before the close was not delivered" }
        expect(p.b.read(Buffer()) == -1) { "read after EOF must keep returning -1" }
    }

    private suspend fun writeToClosedPeer(p: StreamPair) {
        p.b.close()
        val chunk = pattern(64 * 1024)
        var failed = false
        repeat(2_000) {
            if (failed) return@repeat
            try { p.a.write(buf(chunk)) } catch (e: IoException) { failed = true } catch (e: CancellationException) { throw e }
            if (!failed) delay(1)
        }
        expect(failed) { "writes to a closed peer kept succeeding" }
    }

    private suspend fun closeRules(p: StreamPair) = coroutineScope {
        val parked = async { runCatching { p.a.read(Buffer()) }.exceptionOrNull() }
        delay(50)
        p.a.close()
        p.a.close()                                          // idempotent
        val e = parked.await()
        expect(e is ClosedException) { "a parked read must end with ClosedException, got $e" }
        expect(runCatching { p.a.read(Buffer()) }.exceptionOrNull() is ClosedException) { "read after close must throw ClosedException" }
        expect(runCatching { p.a.write(buf("x")) }.exceptionOrNull() is ClosedException) { "write after close must throw ClosedException" }
    }

    private suspend fun concurrentRead(p: StreamPair) = coroutineScope {
        val first = async { runCatching { p.a.read(Buffer()) } }
        delay(50)
        val second = runCatching { p.a.read(Buffer()) }.exceptionOrNull()
        expect(second is IllegalStateException) { "a second concurrent read must throw IllegalStateException, got $second" }
        p.b.write(buf("ok"))
        val r = first.await()
        expect(r.getOrNull() == 2) { "the first read must still get the data, got $r" }
    }

    private suspend fun concurrentWrite(p: StreamPair) = coroutineScope {
        // Park a write by not reading on the peer; grow it until it does park.
        var size = 1 shl 20
        while (size <= 64 shl 20) {
            val first = async { runCatching { p.a.write(buf(ByteArray(size))) } }
            delay(50)
            if (first.isActive) {
                val second = runCatching { p.a.write(buf("x")) }.exceptionOrNull()
                expect(second is IllegalStateException) { "a second concurrent write must throw IllegalStateException, got $second" }
                val drain = launch { readExactly(p.b, size) }    // drain so the first can finish
                expect(first.await().isSuccess) { "the first write failed after a refused second one" }
                drain.join()
                return@coroutineScope
            }
            first.await()
            size *= 4
        }
        // A transport that never parks a write (unbounded buffering) cannot show this rule.
    }

    private suspend fun writevOrder(p: StreamPair) {
        val parts = arrayOf(buf("one-"), buf("two-"), buf("three"))
        val n = p.a.writev(parts, 3)
        expect(n == 13L) { "writev returned $n" }
        expect(parts.all { it.readableBytes == 0 }) { "writev must empty every buffer" }
        expect(readExactly(p.b, 13).decodeToString() == "one-two-three") { "writev changed the order" }
    }

    private suspend fun splitFuzz(p: StreamPair) = coroutineScope {
        val data = pattern(64 * 1024, seed = 11)
        val reader = async { readExactly(p.b, data.size) }
        val r = Random(3)
        var i = 0
        while (i < data.size) {
            val n = minOf(1 + r.nextInt(1500), data.size - i)
            p.a.write(buf(data.copyOfRange(i, i + n)))
            i += n
            if (r.nextInt(8) == 0) yield()
        }
        expect(reader.await().contentEquals(data)) { "a split write arrived changed" }
    }

    private suspend fun cancelledWriteAccounting(p: StreamPair) = coroutineScope {
        val size = 32 shl 20
        val src = buf(ByteArray(size) { (it % 251).toByte() })
        val writer = launch { runCatching { p.a.write(src) } }
        delay(50)
        writer.cancel(); writer.join()
        val sent = size - src.readableBytes
        if (StreamCapability.ResumableAfterCancel in p.a.capabilities) {
            orderlyClose(p.a)
            val got = readToEof(p.b)
            expect(got.size == sent) { "src says $sent bytes were sent, the peer received ${got.size}" }
            for (k in got.indices) if (got[k] != (k % 251).toByte()) fail("byte $k differs")
        } else {
            // The cancel closed the stream, possibly mid-message: the peer may see an error instead of EOF,
            // but it must never receive bytes that src still counts as unsent.
            p.a.close()
            val acc = Buffer()
            try {
                while (true) { val r = Buffer(); if (p.b.read(r) < 0) break; acc.writeBytes(r.readAll()) }
            } catch (_: IoException) {
            }
            val got = acc.readAll()
            expect(got.size <= sent) { "the peer received ${got.size} bytes but src says only $sent were sent" }
            for (k in got.indices) if (got[k] != (k % 251).toByte()) fail("byte $k differs")
        }
    }

    private suspend fun resetIsError(p: StreamPair) {
        p.b.close()                                           // the factory made this end reset (RST)
        delay(50)
        val e = runCatching { readToEof(p.a) }.exceptionOrNull()
        expect(e is IoException && e !is ClosedException) { "a reset must surface as IoException, got ${e ?: "EOF"}" }
    }

    // ---- optional ---------------------------------------------------------------------------

    private suspend fun halfClose(p: StreamPair) {
        if (StreamCapability.HalfClose in p.a.capabilities) {
            p.a.write(buf("before"))
            p.a.shutdownOutput()
            expect(readToEof(p.b).decodeToString() == "before") { "the peer must read the data, then EOF" }
            p.b.write(buf("back"))
            expect(readExactly(p.a, 4).decodeToString() == "back") { "this side must still read after shutdownOutput" }
        } else {
            expect(runCatching { p.a.shutdownOutput() }.exceptionOrNull() is UnsupportedOperationException) { "undeclared HalfClose must be refused" }
        }
    }

    private suspend fun readTimeout(p: StreamPair) {
        val caps = p.a.capabilities
        if (StreamCapability.ReadTimeout in caps) {
            p.a.setReadTimeout(100)
            val e = runCatching { p.a.read(Buffer()) }.exceptionOrNull()
            expect(e is TimeoutException) { "a read past its timeout must throw TimeoutException, got $e" }
            p.a.setReadTimeout(0)
            if (StreamCapability.ResumableAfterCancel in caps) {
                p.b.write(buf("y"))
                expect(readExactly(p.a, 1).decodeToString() == "y") { "the stream must stay usable after a read timeout" }
            }
        } else {
            expect(runCatching { p.a.setReadTimeout(100) }.exceptionOrNull() is UnsupportedOperationException) { "undeclared ReadTimeout must be refused" }
            p.a.setReadTimeout(0)                                // 0 = none, always accepted
        }
    }

    private suspend fun writeIdleTimeouts(p: StreamPair) {
        val caps = p.a.capabilities
        val w = runCatching { p.a.setTimeouts(writeTimeoutMillis = 1_000) }.exceptionOrNull()
        if (StreamCapability.WriteTimeout in caps) expect(w == null) { "declared WriteTimeout was refused: $w" }
        else expect(w is UnsupportedOperationException) { "undeclared WriteTimeout must be refused, got $w" }
        p.a.setTimeouts(0, 0, 0)
        val i = runCatching { p.a.setTimeouts(idleTimeoutMillis = 100) }.exceptionOrNull()
        if (StreamCapability.IdleTimeout in caps) {
            expect(i == null) { "declared IdleTimeout was refused: $i" }
            val e = runCatching { p.a.read(Buffer()) }.exceptionOrNull()
            expect(e is IoException) { "an idle timeout must end a parked read with an IoException (TimeoutException or ClosedException), got $e" }
        } else {
            expect(i is UnsupportedOperationException) { "undeclared IdleTimeout must be refused, got $i" }
        }
    }

    private suspend fun afterCancel(p: StreamPair) = coroutineScope {
        val reader = launch { p.a.read(Buffer()) }
        delay(50)
        reader.cancel(); reader.join()
        if (StreamCapability.ResumableAfterCancel in p.a.capabilities) {
            p.b.write(buf("z"))
            expect(readExactly(p.a, 1).decodeToString() == "z") { "after a cancelled read the stream must deliver later data" }
        } else {
            expect(runCatching { p.a.read(Buffer()) }.exceptionOrNull() is ClosedException) { "without ResumableAfterCancel the stream must close after a cancel" }
        }
    }

    private suspend fun anyThread(p: StreamPair) {
        if (StreamCapability.AnyThread !in p.a.capabilities) return
        val ctx = newSingleThreadContext("io-conformance")
        try {
            withContext(ctx) { p.a.write(buf("from-another-thread")) }
            expect(readExactly(p.b, 19).decodeToString() == "from-another-thread") { "a write from another thread was lost" }
        } finally { ctx.close() }
    }
}
