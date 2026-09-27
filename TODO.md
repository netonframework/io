# neton-io / msgtrans 执行清单（对照 SPEC.md §18 与 msgtrans-kotlin/SPEC.md §10）

> 已停止维护（2026-09-27）：这是 §18–§23 时期的清单。当前执行顺序见 SPEC.md §28.11，章节状态见 SPEC.md 开头的导读。

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

## §20 平台（最终：macOS / Linux / Windows / iOS / Android；neton-io 只做 I/O 与网络，不做 TLS）
- [x] 源码集拆分：nativeMain（expect）/ posixMain / linuxMain / appleMain / mingwMain；macOS 两驱动 33/33
- [x] mingwMain：Winsock + WSAPoll，mingwX64 编译链接通过（未在 Windows 运行）
- [x] Android：androidNative 四个目标恢复；epollMain（Linux + Android 共用 epoll）；全部目标编译通过
- [x] Android 测试：Pixel_9 模拟器（Android 16 / API 36，arm64-v8a）epoll 与 poll 各 33/33
- [x] iOS 模拟器测试（iosSimulatorArm64）33/33
- [x] linuxArm64 测试（colima 的 aarch64 Ubuntu 虚拟机，内核 6.8）epoll / poll / io_uring 各 34/34
- [ ] Windows：IOCP 驱动（性能）；需要 Windows 测试机或 CI
- [ ] 各平台性能基线（同机回显，公平客户端）

## §21 TLS — 移出 neton-io 范围（上层负责）；评估资料见 SPEC §18.5、§21.1
- [ ] linuxX64（153）：epoll / poll 34/34；io_uring 在 Gradle 下出现过 2 次 `io_uring_setup` 失败（AddressTest、UringMappingLeakTest），直接运行 8 次 + 全套 2 次均通过。
      假设：Gradle 守护进程占用约 3 GB 内存时的内存压力（153 共 3.6 GB）；已让失败信息带上 errno（下次出现即可确认）

## §23 补齐功能差距（用户确认 1–7；顺序 23.5 → 23.3 → 23.2 → 23.4 → 23.6 → 23.7 → 23.1）
- [x] 23.5 SocketOptions（listen / listenGroup / connect），getsockopt 读回测试（macOS 两驱动 38/38；153 三驱动 39/39，连接超时用例真实执行）
- [x] 23.3 writev（sendmsg / IORING_OP_SENDMSG / WSASend）+ shutdownOutput，测试（macOS 两驱动 42/42；153 三驱动 43/43，含 io_uring SENDMSG）
- [x] 23.2 计时轮 + 读/写/空闲超时 + 连接超时 + 帧读取速率 + closeGracefully + Framed.feed/批量 flush + io_uring 读侧反压，测试（macOS 两驱动 50/50；153 三驱动 51/51）；流水线深度 16 批量 flush ×3.6（epoll）/ ×4.8（io_uring）。热路径：cachegrind 发现 +14–16% 用户态指令，修复后 +1.1%（epoll）/ +3.6%（io_uring）（docs/benchmarks/2026-09-26-gap-closing.md）
- [x] 23.4 maxConnections / pause / resume / shutdown(graceful) + SO_REUSEPORT 接收模式，测试（macOS 54/54、Linux 三驱动 55/55、Android、iOS）；153 成对 ReusePort ≈ Handoff（0.98–1.02），默认保持 Handoff
- [x] 23.6 Unix 域套接字（POSIX + Windows），测试（Linux 含抽象命名空间；Android shell 不允许 socket 文件，相应用例跳过）；153 上比 TCP 回环快 1.2–1.64 倍
- [x] 23.7 BufferPool + 池化 Buffer + Bytes 零拷贝切片，测试；1000 空闲连接 16 KiB：epoll 30.6 MB → 2.9 MB；池化后指令数与不池化相当（+1.5% / +2.6% vs v25）
- [ ] 23.1 IocpReactor + 可覆盖唤醒：已实现、mingwX64 编译链接通过、已推 ci/windows-validation；**Windows 上运行结果未验证**（GitHub API 仍 404，需查看 Actions 页面的 Windows IOCP / WSAPoll 任务）
