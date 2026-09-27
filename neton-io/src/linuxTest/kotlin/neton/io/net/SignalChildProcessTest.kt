@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class, kotlin.native.concurrent.ObsoleteWorkersApi::class)

package neton.io.net

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.toKString
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.posixshim.neton_signal_disposition
import platform.posix.SIGTERM
import platform.posix.exit
import platform.posix.fclose
import platform.posix.fflush
import platform.posix.fgets
import platform.posix.fopen
import platform.posix.fputs
import platform.posix.getenv
import platform.posix.getpid
import platform.posix.kill
import platform.posix.mkdir
import platform.posix.raise
import platform.posix.readlink
import platform.posix.setenv
import platform.posix.stdout
import platform.posix.system
import platform.posix.unsetenv
import platform.posix.usleep
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * SPEC §27.7 end to end, in a separate process so the test process's own signal handling is never
 * involved: the test binary starts itself in "child" mode. The child serves with
 * `shutdownOnSignals = true`; the parent holds a connection and sends SIGTERM (the child must stay up:
 * graceful), then closes it (the child's serveTcp returns). The child then sends itself SIGTERM, which
 * must now end it by default action: exit status 128 + 15, and it never prints "survived".
 */
class SignalChildProcessTest {
    private fun readFile(path: String): String? = memScoped {
        val f = fopen(path, "r") ?: return@memScoped null
        val buf = allocArray<ByteVar>(4096); val sb = StringBuilder()
        while (fgets(buf, 4096, f) != null) sb.append(buf.toKString())
        fclose(f); sb.toString()
    }

    private fun writeFile(path: String, text: String) {
        val f = fopen("$path.tmp", "w") ?: error("cannot write $path")
        fputs(text, f); fclose(f)
        platform.posix.rename("$path.tmp", path)
    }

    private fun selfPath(): String = memScoped {
        val buf = allocArray<ByteVar>(4096)
        val n = readlink("/proc/self/exe", buf, 4095u)
        check(n > 0) { "readlink /proc/self/exe failed" }
        buf.readBytes(n.toInt()).decodeToString()
    }

    @Test
    fun serveTcpStopsOnSigtermAndRestoresTheDefaultAction() {
        if (getenv("NETON_SIGNAL_CHILD")?.toKString() == "serve") return child()
        runReactor { parent() }
    }

    private fun child() {
        val dir = getenv("NETON_SIGNAL_DIR")!!.toKString()
        // Tell the parent only once our SIGTERM handler is really installed.
        Worker.start(name = "ready").executeAfter(0L) {
            while (neton_signal_disposition(SIGTERM) != 1) usleep(1_000u)
            writeFile("$dir/pid", getpid().toString())
        }
        serveTcp("127.0.0.1", 21970, reactors = 2, shutdownOnSignals = true, shutdownTimeoutMillis = 10_000) { conn ->
            try { val b = Buffer(); while (conn.read(b) >= 0) b.clear() } finally { conn.close() }
        }
        println("child: served"); fflush(stdout)
        raise(SIGTERM)                       // the default action must end the process here
        usleep(2_000_000u)
        println("child: survived"); fflush(stdout)
        exit(0)
    }

    private suspend fun parent() {
        val dir = "/tmp/neton-sigchild-${getpid()}"
        mkdir(dir, 0x1c0u)
        setenv("NETON_SIGNAL_CHILD", "serve", 1); setenv("NETON_SIGNAL_DIR", dir, 1)
        val self = selfPath()
        system("( '$self' --ktest_filter=neton.io.net.SignalChildProcessTest.* > '$dir/out' 2>&1; echo \$? > '$dir/status' ) &")
        unsetenv("NETON_SIGNAL_CHILD"); unsetenv("NETON_SIGNAL_DIR")
        try {
            val pid = withTimeout(10_000) { while (true) { readFile("$dir/pid")?.trim()?.toIntOrNull()?.let { return@withTimeout it }; delay(20) }; @Suppress("UNREACHABLE_CODE") 0 }
            withTimeout(5_000) { while (!tryConnectProbe(21970)) delay(20) }
            val c = connect("127.0.0.1", 21970)
            delay(100)
            assertEquals(0, kill(pid, SIGTERM))
            delay(500)
            assertEquals(0, kill(pid, 0), "the child must still run: the graceful stop waits for our connection")
            c.close()
            val status = withTimeout(15_000) { while (true) { readFile("$dir/status")?.trim()?.takeIf { it.isNotEmpty() }?.let { return@withTimeout it }; delay(50) }; @Suppress("UNREACHABLE_CODE") "" }
            val out = readFile("$dir/out") ?: ""
            assertTrue("child: served" in out, "the child's serveTcp must return after the graceful stop; output:\n$out")
            assertTrue("child: survived" !in out, "SIGTERM after serveTcp returned must end the child; output:\n$out")
            assertEquals("${128 + SIGTERM}", status, "exit status; output:\n$out")
        } finally {
            system("rm -rf '$dir'")
        }
    }
}
