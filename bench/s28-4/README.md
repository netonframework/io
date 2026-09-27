# SPEC §28.4 measurements (§28.11 steps 3–4)

Run on the 153 host (Rocky 9, 4 vCPU): server `taskset -c 0,1` (two reactors), load generator `taskset -c 2,3`.
Binaries: `echoServer.kexe` / `echoServer-cur.kexe` (this step), `echoServer-base.kexe` (fdc99f6), `echoServer-NoGc` /
`-MarkSt` (GC variants), `fairnessProbe.kexe`, `echo-client-mass` (bench/echo-client-mass), geario `server-geario`.

| Script | Scenario | Output |
|---|---|---|
| f2.sh | F2: resume rings kept busy (rotation vs strict) | f2.out |
| l2.sh | L2: saturated fairness, 1000 cold + 64 hot, closed loop | l2.out |
| l1f3.sh | L1 and the first F3 (closed-loop burst; voided, see SPEC) | l1f3.out |
| f3b.sh | F3 rerun (burst of 10 000 frames every 100 ms) | f3b.out (run 15 voided) |
| d1.sh | L1 diagnosis: default / no GC / single-threaded mark / GC stats | d1.out |
| d2.sh | L1 remedy: GC minimum heap 0 / 64 / 256 MiB | d2.out |
| cg3.sh | cachegrind Ir per request, base vs new vs strict | cg3.out |

Results and their reading are in SPEC.md, "§28.11 第 3 步".
