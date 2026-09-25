# echo-client-fair

geario's `bench-echo/client` plus per-connection completion counts (min / p10 / median / max,
connections with zero completions, Jain's fairness index). Used by neton-io SPEC §18.3 to check that
a good p99 is not the result of starving some connections. Build for the Linux bench host:

    cargo build --release --target x86_64-unknown-linux-gnu
