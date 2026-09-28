package neton.io.net

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.io.core.IoException
import neton.io.core.IoStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SmallApiTest {
    @Test
    fun listenerOnPortZeroReportsItsPort() = runReactor {
        val listener = listen("127.0.0.1", 0)
        val port = listener.localAddress.port
        assertTrue(port > 0, "port $port")
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(listener.accept()) }
        val c = connect("127.0.0.1", port)
        val s = accepted.await()
        c.close(); s.close(); listener.close()
    }

    @Test
    fun aResetIsRecognised() = runReactor {
        val listener = listen("127.0.0.1", 0)
        val accepted = CompletableDeferred<IoStream>()
        launch { accepted.complete(listener.accept()) }
        val c = connect("127.0.0.1", listener.localAddress.port, SocketOptions(lingerSeconds = 0))
        val s = accepted.await()
        c.close()                                               // SO_LINGER 0: the close is a reset
        val e = runCatching { while (s.read(Buffer()) >= 0) { } }.exceptionOrNull()
        assertTrue(e is IoException && e.isConnectionReset, "expected a reset, got $e")
        s.close(); listener.close()
    }

    @Test
    fun bytesCopyARange() {
        val b = Bytes.copyOf("hello world".encodeToByteArray()).slice(6)
        val dst = ByteArray(5) { '.'.code.toByte() }
        b.copyInto(dst, 1, 1, 4)
        assertEquals(".orl.", dst.decodeToString())
        assertContentEquals("world".encodeToByteArray(), ByteArray(5).also { b.copyInto(it, 0, 0, 5) })
    }
}
