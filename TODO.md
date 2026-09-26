# neton-io / msgtrans 执行清单（对照 SPEC.md §18 与 msgtrans-kotlin/SPEC.md §10）

规则：每项先有 SPEC 条目；一次一个变量；Linux 结果以 153 为准（三驱动），macOS 本机；基准用成对交替轮次。

## 阶段 1
- [x] 18.1 `listenGroup` / `TcpServerGroup`（neton-io 公共 API），`serveTcp` 改为基于它
- [x] 18.1 测试：serve 在 close 后返回；handler 分布到 ≥2 线程；单 reactor 留在调用线程（macOS 23/23；153 三驱动 28/28）
- [x] msgtrans §10 `Transport.bind(..., reactors = N)`；连接作用域 = 目标 reactor 调度器 + SupervisorJob(父 = 服务作用域)
- [x] msgtrans §10 测试：多 reactor bind、连接分布、关闭服务时所有 reactor 上的连接都关闭（macOS 34；153 io_uring/epoll 各 34/34）
- [x] msgtrans framed/rpc 基准：1 vs 4 reactor（153，成对）：framed 1.46–1.69×，rpc 1.13–1.20×，**未达 2×**；假设为压测客户端占满核，CPU 诊断已排队
- [x] 18.2 IPv6 字面量 + 双栈 listen
- [x] 18.2 `getaddrinfo` 解析 Worker + connect 逐个尝试地址（字面量 AI_NUMERICHOST 内联；平台地址代码合并为通用实现）
- [x] 18.2 测试：localhost、IPv6 回环、双栈、无法解析 → ConnectException（macOS 通过，本机 fake-IP 下自动跳过；153 三驱动 28/28，解析失败路径真实执行）
- [ ] 18.3 压测矩阵脚本（连接数 × 载荷 × RSS/p99）并跑一轮

## 待决
- [ ] 18.5 密码学原语：已评估 cryptography-kotlin、openssl-kotlin、native-builds；建议 native-builds 供给 libcrypto 3.6.4 + 记录层自写 EVP 薄绑定 + 握手用 cryptography-kotlin（同一份 libcrypto，已验证）。待定：TLS 协议本身用 Kotlin 写还是用 OpenSSL libssl

## §19 工具链与运行时
- [x] 19.1 工具链：用户决定固定 Kotlin 2.4.0（2.4.10/2.4.20 无 Native 运行时改动），以后再议
- [x] 19.1 153 成对：不做（版本固定）
- [x] 19.2 gc=pmcs：单核 1.034（9/12），四核 0.956 → 不采用
- [x] 19.2 gcMarkSingleThreaded=true：二进制与基线逐字节相同（2.4.0/CMS 下无效）→ 不测
- [x] 19.2 preCodegenInlineThreshold=40：单核 1.032（9/12）→ 用于服务端可执行文件
- [x] 19.2 -Xklib-ir-inliner=full：0.992，无效 → 不采用
- [x] 19.3 就绪驱动：续体队列 + 每流一次的取消登记（b5c441a；macOS 31/31；153 三驱动 32/32、msgtrans io_uring/epoll 34/34；基准在队列）
- [x] 19.3 io_uring 驱动同上（槽内原始续体 + Int 恢复环 + 每 fd 每作业取消登记；153 io_uring 34/34 ×3、msgtrans 41/41 ×2；基准在队列）
- [x] 19.4 自适应读大小 + 回显按需缓冲：1000 连接 RSS 134.6→15.9 MB（每连接 138→16 KB），64 KB 吞吐不降 → 采用
- [x] 18.3 修复：io_uring 64 KB 卡顿（约 42 ms）→ §19.6 TCP_NODELAY，25 → 11,172 qps
- [x] 18.3 核实：高连接数下 epoll p99 是否掩盖不公平 → 是（Jain 0.29–0.40），§19.5 修复后 Jain 0.99+；多核领先结论撤回
- [ ] 公平前提下的剩余差距：×4/12 连接（0.87）、io_uring 整体（0.76–0.91）、1000 连接 p99（10–14 ms vs 7 ms）
- [ ] 测试端口移出临时端口范围后，153 三驱动复跑

## §20 平台（收窄：macOS / Linux / Windows 必须，iOS 保留）
- [x] P1 源码集拆分：nativeMain 只放 expect，posixMain（Linux + Apple）放 POSIX socket，linuxMain 放 epoll + io_uring；非 Windows 目标全部编译，macOS 两驱动 33/33
- [ ] P1 mingwMain：Winsock 的 socket/地址/唤醒实现 + WSAPoll 驱动，mingwX64 可编译链接
- [ ] P2 linuxArm64 测试（colima arm64 容器）
- [ ] P3 Windows 测试机（待用户提供）→ WSAPoll 打通 → IOCP 驱动（性能）

## §21 TLS 1.3
- [ ] 依赖接入（native-builds libcrypto 3.6.4 + cryptography-kotlin nativebuilds provider + 自写 EVP 声明），20 目标可编译
- [ ] 记录层（常驻 EVP 上下文、原地加解密）+ RFC 8448 向量
- [ ] 握手状态机（客户端、服务端）
- [ ] X.509 与各平台信任锚
- [ ] 互通（openssl、Go）与性能对比（geario + rustls）
