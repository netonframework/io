package neton.io.testkit

import neton.io.core.IoStream
import neton.io.core.memoryStreamPair

/** Duplex pair `(client, server)`; kept for the older tests, now [memoryStreamPair] (SPEC §27.4). */
fun memoryPair(): Pair<IoStream, IoStream> = memoryStreamPair()
