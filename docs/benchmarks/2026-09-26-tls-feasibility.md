# TLS in Kotlin/Native — feasibility and performance estimate vs rustls (153, 2026-09-26)

All numbers: 153 (4 vCPU AMD EPYC, Rocky 9.8), one core pinned (`taskset -c 1`), medians of repeated runs.

## Measured building blocks

| operation | aws-lc-rs (rustls default) | C → OpenSSL 3.5.5 | Kotlin/Native → OpenSSL 3.6.4 |
|---|---|---|---|
| seal one 128 B record (AES-128-GCM, 12 B nonce, 5 B AAD, tag) | 176 ns | 220 ns | 285 ns |
| seal one 1 KB record | 416 ns | 460 ns | 513 ns |
| seal one 16 KB record | 4,390 ns | 4,400 ns | 4,435 ns |
| X25519 keygen + agree (one handshake side) | 37.7 µs | ≈ 71 µs (`openssl speed`: 35.7 µs per derive) | same as OpenSSL |
| ECDSA P-256 sign | 19.1 µs | 24.0 µs | same as OpenSSL |
| ECDSA P-256 verify | 61 µs | 72 µs | same as OpenSSL |

Kotlin/Native's own cost over C for the same EVP sequence: +35–65 ns per record (pinning four arrays and five
cinterop calls in a naive loop; pinning the connection's buffers once should cut it).

## rustls itself (rustls-bench, main @ 99f2358, aws-lc-rs, ECDSA P-256, buffered API, `--threads 1`)

| | rustls |
|---|---|
| bulk TLS 1.3 AES-128-GCM send / recv | 3,328 / 3,154 MB/s |
| full handshake, server / client | 8,931 / 3,238 per s (≈ 112 / 309 µs) |
| ticket resumption, server / client | 10,361 / 8,576 per s |
| memory per endpoint (2,000 → 20,000 connections) | ≈ 13 KB |

## Estimate for a rustls-style Kotlin/Native TLS (Kotlin protocol + OpenSSL primitives)

- **Bulk data (16 KB records): parity, 0.95–1.0× rustls.** Both are AES-NI bound; the per-record primitive cost is
  4,390 vs 4,435 ns.
- **Small records (128 B): +≈110 ns per record** (285 vs 176 ns). For msgtrans-style RPC at ~120k req/s per core
  (≈ 8.3 µs per request, two records) that is ≈ −2.5% throughput vs a rustls-like design; ≈ −1.5% if buffers stay pinned.
- **Full handshakes: ≈ 0.7–0.75× rustls on the server, ≈ 0.85× on the client.** The gap is the primitives, not
  Kotlin: OpenSSL's X25519 is ≈ 2× slower than aws-lc's and its ECDSA ≈ 20% slower (server: 112 µs + ≈ 33 µs X25519
  + ≈ 5 µs sign ≈ 150 µs). Kotlin's own handshake logic and allocations add an estimated 10–20 µs on top.
- **Memory per connection: comparable (estimated 5–20 KB)** with the §19.4 adaptive-buffer approach: two cipher
  contexts plus buffers sized to the traffic; handshake state freed after the handshake.
- To reach handshake parity, aws-lc can replace OpenSSL behind the same thin-call layer (it is C as well); it is not
  shipped by native-builds, so it would be built for our targets ourselves.

These are estimates from measured parts; the real numbers come from running rustls-bench's equivalent scenarios
against the Kotlin implementation once it exists.
