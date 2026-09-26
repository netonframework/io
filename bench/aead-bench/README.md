# aead-bench

SPEC §18.5 evidence: cost of sealing one TLS-style record (AES-128-GCM, explicit 12-byte nonce, 5-byte AAD,
16-byte tag) through cryptography-kotlin 0.6.0 (OpenSSL provider, `encryptWithIvBlocking`) versus direct
libcrypto calls with one `EVP_CIPHER_CTX` per connection. Both link the same OpenSSL (the provider's
prebuilt 3.6.0): the cinterop here is declarations only. Throwaway tooling, not part of the neton-io build;
headers come from Homebrew's `openssl@3`.

    ./gradlew linkReleaseExecutableLinuxX64   # or ...MacosArm64
