package neton.io.net

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import platform.posix.fclose
import platform.posix.fgets
import platform.posix.fopen
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * P1-7 regression: the reactor must release everything it acquired at startup. The io_uring
 * driver mmaps three regions (SQ ring, CQ ring, SQEs) per reactor; before the fix `shutdown()`
 * closed the ring fd but never unmapped them, so every reactor leaked three mappings of the
 * process address space — invisible to the functional tests, fatal to a long-lived host that
 * creates reactors per task.
 *
 * Counting lines in /proc/self/maps measures exactly that: with the leak, N reactors leave 3N
 * extra mappings behind; with the fix the count is flat. Readiness drivers (epoll/poll) map
 * nothing and pass trivially, which is the correct expectation for them.
 */
class UringMappingLeakTest {

    @OptIn(ExperimentalForeignApi::class)
    private fun mappingCount(): Int = memScoped {
        val f = fopen("/proc/self/maps", "r") ?: return 0
        val buf = allocArray<kotlinx.cinterop.ByteVar>(4096)
        var n = 0
        while (fgets(buf, 4096, f) != null) n++
        fclose(f)
        n
    }

    private fun cycleReactors(times: Int) {
        repeat(times) { runReactor { } }
    }

    @Test
    fun reactorShutdownReleasesItsMappings() {
        // Warm up first: the first reactor pulls in lazily-mapped runtime pages, which would
        // otherwise be charged to the measured window.
        cycleReactors(20)
        val before = mappingCount()

        val cycles = 100
        cycleReactors(cycles)
        val after = mappingCount()

        val growth = after - before
        // The leak would be 3 per reactor (300 here). Allow generous slack for allocator arenas.
        assertTrue(
            growth < cycles,
            "reactor shutdown leaked address-space mappings: $cycles reactors added $growth " +
                "mappings ($before -> $after); expected roughly flat"
        )
    }
}
