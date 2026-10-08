package neton.io.net

/**
 * WSAECONNRESET (10054); and, from IOCP, what a pending overlapped operation ends with when the peer tears the
 * connection down: WSAECONNABORTED (10053, seen by a WebSocket client reading after the server dropped the connection
 * in CI), ERROR_NETNAME_DELETED (64) and ERROR_CONNECTION_ABORTED (1236) — the codes IOCP accept already treats as a
 * vanished connection (SPEC §33.2).
 */
internal actual fun isConnectionResetErrno(errno: Int): Boolean = errno == 10054 || errno == 10053 || errno == 64 || errno == 1236
