# neton-io / msgtrans 执行清单（对照 SPEC.md §18 与 msgtrans-kotlin/SPEC.md §10）

规则：每项先有 SPEC 条目；一次一个变量；Linux 结果以 153 为准（三驱动），macOS 本机；基准用成对交替轮次。

## 阶段 1
- [x] 18.1 `listenGroup` / `TcpServerGroup`（neton-io 公共 API），`serveTcp` 改为基于它
- [x] 18.1 测试：serve 在 close 后返回；handler 分布到 ≥2 线程；单 reactor 留在调用线程（macOS；Linux 待 153）
- [ ] msgtrans §10 `Transport.bind(..., reactors = N)`；连接作用域 = 目标 reactor 调度器 + SupervisorJob(父 = 服务作用域)
- [ ] msgtrans §10 测试：多 reactor bind、连接分布、关闭服务时所有 reactor 上的连接都关闭
- [ ] msgtrans framed/rpc 基准：1 vs 4 reactor（153，成对）
- [ ] 18.2 IPv6 字面量 + 双栈 listen
- [ ] 18.2 `getaddrinfo` 解析 Worker + connect 逐个尝试地址
- [ ] 18.2 测试：localhost、IPv6 回环、无法解析 → ConnectException
- [ ] 18.3 压测矩阵脚本（连接数 × 载荷 × RSS/p99）并跑一轮

## 待决
- [ ] 18.5 密码学原语：纯 Kotlin 还是薄调用 libcrypto（用户决定；阶段 2 等待）
