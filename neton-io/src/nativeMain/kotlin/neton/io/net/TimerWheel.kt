package neton.io.net

/**
 * A node that can sit in a [TimerWheel] (intrusive: scheduling allocates nothing). SPEC §23.2.
 */
internal abstract class WheelNode {
    internal var wheelDueMs: Long = 0
    internal var wheelSlot: Int = -1          // -1 = not scheduled
    internal var wheelPrev: WheelNode? = null
    internal var wheelNext: WheelNode? = null

    /** The node is due at [nowMs]. Return the next due time (> nowMs) to stay scheduled, or 0 to leave the wheel. */
    internal abstract fun onWheelDue(nowMs: Long): Long
}

/**
 * Hashed timing wheel for connection timeouts (SPEC §23.2): [slots] buckets of [tickMs] each. Nodes
 * keep their own deadlines; the owner only reschedules when a deadline moves *earlier* than the
 * scheduled time, so the per-operation cost is a field write. A node scheduled too early is simply
 * re-evaluated and rescheduled when its slot comes round. Reactor-thread only.
 */
internal class TimerWheel(private val tickMs: Long = 10, private val slots: Int = 512) {
    private val heads = arrayOfNulls<WheelNode>(slots)
    private var lastTick = -1L
    var size = 0
        private set
    /** Lower bound on the next due time (ms), for the poll timeout; Long.MAX_VALUE when empty. */
    var nextDueMs = Long.MAX_VALUE
        private set

    fun schedule(node: WheelNode, dueMs: Long) {
        if (node.wheelSlot >= 0) {
            if (dueMs >= node.wheelDueMs) return        // already due no later than asked
            unlink(node)
        }
        val slot = ((dueMs / tickMs) % slots).toInt()
        node.wheelDueMs = dueMs; node.wheelSlot = slot
        node.wheelPrev = null; node.wheelNext = heads[slot]
        heads[slot]?.wheelPrev = node
        heads[slot] = node
        size++
        if (dueMs < nextDueMs) nextDueMs = dueMs
    }

    fun cancel(node: WheelNode) { if (node.wheelSlot >= 0) unlink(node) }

    private fun unlink(node: WheelNode) {
        val p = node.wheelPrev; val n = node.wheelNext
        if (p != null) p.wheelNext = n else heads[node.wheelSlot] = n
        n?.wheelPrev = p
        node.wheelPrev = null; node.wheelNext = null; node.wheelSlot = -1
        size--
    }

    /** Fire every node due at or before [nowMs]. */
    fun tick(nowMs: Long) {
        if (size == 0) { lastTick = nowMs / tickMs; nextDueMs = Long.MAX_VALUE; return }
        if (nowMs < nextDueMs) return
        val nowTick = nowMs / tickMs
        val from = if (lastTick < 0) nowTick else lastTick
        val steps = if (nowTick - from >= slots) slots.toLong() else nowTick - from + 1
        var t = from
        for (k in 0 until steps) {
            val slot = (t % slots).toInt()
            var node = heads[slot]
            while (node != null) {
                val next = node.wheelNext
                if (node.wheelDueMs <= nowMs) {
                    unlink(node)
                    val again = node.onWheelDue(nowMs)
                    if (again > nowMs) schedule(node, again)
                }
                node = next
            }
            t++
        }
        lastTick = nowTick
        recomputeNextDue(nowTick)
    }

    /** Earliest due time among nodes in the slots ahead (one pass over the buckets, once per tick). */
    private fun recomputeNextDue(nowTick: Long) {
        if (size == 0) { nextDueMs = Long.MAX_VALUE; return }
        var best = Long.MAX_VALUE
        for (k in 0 until slots) {
            var node = heads[((nowTick + k) % slots).toInt()]
            while (node != null) { if (node.wheelDueMs < best) best = node.wheelDueMs; node = node.wheelNext }
            if (best != Long.MAX_VALUE && best <= (nowTick + k + 1) * tickMs) break
        }
        nextDueMs = best
    }
}
