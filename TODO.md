# neton-io / msgtrans 执行清单（对照 SPEC.md §18 与 msgtrans-kotlin/SPEC.md §10）

规则：每项先有 SPEC 条目；一次一个变量；Linux 结果以 153 为准（三驱动），macOS 本机；基准用成对交替轮次。

## 阶段 1
- [x] 18.1 `listenGroup` / `TcpServerGroup`（neton-io 公共 API），`serveTcp` 改为基于它
- [x] 18.1 测试：serve 在 close 后返回；handler 分布到 ≥2 线程；单 reactor 留在调用线程（macOS；Linux 待 153）
- [x] msgtrans §10 `Transport.bind(..., reactors = N)`；连接作用域 = 目标 reactor 调度器 + SupervisorJob(父 = 服务作用域)
- [x] msgtrans §10 测试：多 reactor bind、连接分布、关闭服务时所有 reactor 上的连接都关闭（macOS；Linux 待 153）
- [ ] msgtrans framed/rpc 基准：1 vs 4 reactor（153，成对）
- [x] 18.2 IPv6 字面量 + 双栈 listen
- [x] 18.2 `getaddrinfo` 解析 Worker + connect 逐个尝试地址（字面量 AI_NUMERICHOST 内联；平台地址代码合并为通用实现）
- [x] 18.2 测试：localhost、IPv6 回环、双栈、无法解析 → ConnectException（macOS 通过；本机 Clash fake-IP 下解析测试自动跳过，真实验证在 153）
- [ ] 18.3 压测矩阵脚本（连接数 × 载荷 × RSS/p99）并跑一轮

## 待决
- [ ] 18.5 密码学原语：纯 Kotlin 还是薄调用 libcrypto（用户决定；阶段 2 等待）

## §19 工具链与运行时
- [ ] 19.1 Kotlin 2.4.20 + coroutines 1.11.0（neton-io / msgtrans / pulsekit 同步），全部测试
- [ ] 19.1 153 成对：2.4.20 对 2.4.0（同一代码）
- [ ] 19.2 gc=pmcs
- [ ] 19.2 gcMarkSingleThreaded=true
- [ ] 19.2 preCodegenInlineThreshold=40
- [ ] 19.2 -Xklib-ir-inliner=full
- [ ] 19.3 就绪驱动：续体队列 + 每流一次的取消登记
- [ ] 19.3 io_uring 驱动同上
