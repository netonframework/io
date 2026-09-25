# neton-io — 规格说明（SPEC）

> **协程原生的高性能异步 I/O 框架（Kotlin/Native 优先），Neton / Pulse 全栈的统一 I/O 底座。**
>
> 架构对标并移植 [`geario`](../../Neton/geario)（ntex 网络层的单 crate 提取，性能 ≈ ntex），
> 采用 ntex-io 的 **buffered filter** 模型，映射到 Kotlin 协程。
> 性能北极星：Golang 最强协程网络库（**gnet / 字节 netpoll**）一档；Rust `ntex/geario` 为参考天花板。
>
> 状态：草案 v0（API 未稳定）· 日期：2026-09-12 · 作者：zoujiaqing

---

## 1. 定位与目标

`neton-io` 是一层**独立的异步 I/O 框架**，不含任何业务/Web 框架逻辑。它向上支撑：

```
                         neton-io
        ┌───────────────────┼────────────────────┐
   msgtrans-kotlin      neton-io-http          （其它）
   长连接 / RPC / 流      HTTP/1.1 · HTTP/2
        │                   │
   Pulse                neton-http（替代 hyper4k / Ktor）
```

**三个战略目标：**

1. **承接 `msgtrans-kotlin`**：长连接、双向 RPC、事件流的传输底座（Pulse 上报 / 远程配置下发 / 命令）。
2. **承接 HTTP 实现**：`neton-io-http` 提供 HTTP/1.1 + HTTP/2，让 `neton-http` 框架**甩掉 Rust 的 `hyper4k`（Tokio+Hyper FFI）**，全栈归一到纯 Kotlin/Native。
3. **未来替代 Ktor**：以更先进的架构（io_uring + buffered-filter + thread-per-core + 零拷贝 + 协程原生）提供比 Ktor 更强、更快、且与长连接/RPC 统一在同一 runtime 的 HTTP 能力。

**为什么能比 Ktor 更强：** Ktor 的 native 引擎（CIO）是 selector/readiness、JVM 血统的实现；`neton-io` 从 geario 继承的是 **io_uring 优先、buffered-filter、每核 reactor、TLS 即 filter** 的模型，且 HTTP 与 RPC/长连接**共用一个 I/O runtime**——Ktor 做不到后者。

---

## 2. 非目标（Non-goals）

- **不**一步到位复刻 Tokio 全部特性；按 §11 分阶段。
- **不** JVM 优先：Kotlin/Native 优先（KMP，后续可加 JVM 后端）。
- **不**做纯 KN 的 TLS：TLS 走原生（OpenSSL / OS，不用 BoringSSL 等第三方），见 §7。
- **不**在公开 API 暴露 `Future/poll/Waker`：对用户只暴露 `suspend` / `Flow`（见 §5）。

---

## 3. 设计原则

1. **协程原生**：`suspend` + `Flow` + structured concurrency + cancellation；不把 Rust 的 poll 模型泄漏给用户。
2. **buffered filter 模型（ntex-io）**：driver 只填/抽 buffer；上层是 buffer 处理 + Filter 栈 + Dispatcher + Service。就绪与处理解耦——这正是它易映射到协程、且少 syscall/少拷贝的原因。
3. **thread-per-core reactor + 连接亲和（gnet Multi-Reactor 模型）**：main-reactor 只负责 `accept`，round-robin 把 fd 派发给多个 sub-reactor；每个 sub-reactor 绑定一个固定 KN Worker + 一个 epoll/kqueue 实例；连接**终生钉死**在同一 sub-reactor 线程。热路径 share-nothing、无跨线程锁（契合 msgtrans 的 per-connection actor）。
   - **亲和性不许破坏**：`read/decode/framing` 的协程**就在本 reactor 的 dispatcher 上 `resume`**，不 hop 线程池；只有重业务 handler 才 **opt-in offload** 到独立协程池。每次 read 都跳 `Dispatchers.Default` = 把亲和红利赔光，禁止。
4. **零拷贝 + off-heap 池化 buffer**：native/pinned 内存 + `readv/writev` + buffer 复用，压制 KN GC 热路径分配。
   - 🔴 **不用 Ktor-IO / 托管堆 `ByteArray` 做 I/O buffer**（托管堆分配 + JVM 血统，既违背 off-heap 又与"替代 Ktor"矛盾）。
   - geario 的 `BytePages`/`FilterBuf` **本身就是 netpoll `LinkBuffer` 式的分页/链式零拷贝 buffer**——直接从 geario 移植（底层换 KN native 内存），无需另复刻 netpoll。
5. **驱动可插拔、运行时选择**：polling/uring/iocp/nw；Linux 运行期有 io_uring 用 io_uring、否则 polling；显式命名的驱动创建失败即报错，**不静默降级**（沿用 geario 规则）。
6. **链接期可裁剪**：靠链接器的死代码消除，不靠拆 artifact（§4）——PulseKit 移动端最终二进制里没有 server/uring/http 的代码，因为它们不可达。

---

## 4. 分层（一个 artifact 内的包）

> 架构学 geario，**发布粒度也学 geario：一个 artifact** `com.netonstream:neton-io`（和 tokio 一样）。
> 曾按 ntex 的多 crate 拆成多个 Gradle 模块，理由是「拆分才能裁剪体积」——这个理由不成立：
> Kotlin/Native 在链接最终二进制时按可达性做死代码消除，一个 klib 里没用到的层同样会被剥掉，
> 拆不拆 artifact 对体积没有影响。下表因此描述的是**包**（`neton.io.*`）的分层，不是坐标。

| 层（包） | 内容 | 对应 geario |
|------|------|-------------|
| `neton-io-bytes` | 引用计数 / off-heap buffer、`Bytes`/`BytesMut`/`BytePages`、`ByteString` | `bytes` |
| `neton-io-codec` | `Decoder` / `Encoder` 接口、常用 codec（LengthDelimited、Bytes、Line） | `codec` |
| `neton-io-core` | **filter I/O 模型**：`Io`/`IoRef`/`Filter`/`Layer`/`Base`/`Framed`/`Readiness`/`IoContext` | `io` |
| `neton-io-service` | `Service<Req,Res>`、pipeline、middleware | `service` |
| `neton-io-dispatcher` | framed 协议的通用连接循环 | `dispatcher` |
| `neton-io-net` | socket、connector、DNS、**平台驱动**（见 §6） | `net` |
| `neton-io-tls` | TLS filter（OpenSSL / Network.framework / SChannel） | `tls/rustls`（重造后端） |
| `neton-io-server` | worker pool + accept loop | `server` |
| `neton-io-rt` | runtime / arbiter / driver 选择 / 定时器 | `rt` |
| `neton-io-http` | **HTTP/1.1 + HTTP/2**（filter/dispatcher 之上；未来替代 Ktor/hyper4k） | geario-http（独立仓） |
| `neton-io-testkit` | **内存 driver** + conformance 测试（Phase 0 零 cinterop） | `io/testing.rs` |

依赖：`msgtrans-kotlin → core+codec+dispatcher+net`；`neton-io-http → core+codec+dispatcher+http`；`PulseKit → msgtrans-kotlin`（裁剪到 client+ws）。

---

## 5. 核心模型与 poll→suspend 翻译契约

geario/ntex 是 `task::Poll` + `poll_read/poll_write` + `Waker` 的手动 poll 模型；`neton-io` 对**内部**保留就绪驱动的等价物，对**用户**只暴露协程。

| geario / ntex-io | neton-io（Kotlin） |
|---|---|
| `Io<F>` / `IoRef` | `Io` / `IoRef`（持有读写 buffer + filter 栈） |
| `Filter` / `FilterLayer` / `Layer<F1,F2>` / `Base` | `Filter` / `FilterLayer` 接口 + `Layer` 组合 |
| `Framed<Io, Codec>` | `Framed`（`Flow<Decoded>` 读、`suspend send()` 写） |
| `Readiness{Ready,Shutdown,Terminate}` | 同名 enum |
| `Service<Req,Res>`（poll_ready + call） | `fun interface Service { suspend fun call(req): Res }` + `readiness: StateFlow` |
| `Driver` / `Notify` / `PollResult{Ready,Pending,PollAgain}` | reactor + `Notify`；内部就绪信号 |
| `Waker`（`testing.rs` 的 `AtomicWaker`） | `CancellableContinuation`（`suspendCancellableCoroutine`） |
| runtime `block_on` / arbiter | 自定义 `CoroutineDispatcher`（绑定 reactor 线程）+ structured concurrency |

**边界契约**：一次 `read/write` 的挂起点用 `suspendCancellableCoroutine` 向 reactor 注册兴趣；reactor 在就绪/完成时 `resume` 该 continuation。连接生命周期用 structured concurrency 管理，cancellation 向下传播关闭 fd。**用户永远看不到 poll**。

---

## 6. 驱动后端矩阵

| 驱动 | 平台 | 机制 | 阶段 |
|------|------|------|------|
| `testing` | 任意（内存） | 内存管道 + continuation | **P0** |
| `polling` | Linux / macOS / iOS / BSD | epoll / kqueue（readiness） | **P1** |
| `nw` | Apple 客户端 | Network.framework（自带 TLS/happy-eyeballs/后台态） | P2 |
| `uring` | Linux | io_uring（completion，天花板） | P3 |
| `iocp` | Windows | IOCP（completion） | P3 |

- **运行时选择**：Linux 有 io_uring 用之、否则 polling；`neton-io.driver=polling|uring|iocp` 可显式指定，指定失败即报错（不静默替换）。
- **客户端 vs 服务端**：Linux 服务端走 reactor（polling/uring）；iOS 客户端优先 `nw`（单连接、Apple 强约束）。**一套 API、按角色多后端**。

---

## 7. TLS 即 Filter

沿用 ntex 把 TLS 建模成 `FilterLayer` 的做法（`Io<Layer<TlsServerFilter, F>>`），替换被关在一个 filter 内、不污染其它层：

- 服务端：**OpenSSL**（cinterop，直接用官方 OpenSSL，不用 BoringSSL）。
- Apple 客户端：**Network.framework**（TLS 内建）。
- Windows：**SChannel**。

> geario 用 rustls（纯 Rust）——`neton-io` 唯一**不能** 1:1 移植的模块，必须换原生后端;但因是 filter,改动被隔离。

---

## 8. HTTP 能力（替代 Ktor / hyper4k 的路径）

`neton-io-http`：
- HTTP/1.1 解析/编码作为 codec + dispatcher；HTTP/2 作为 filter/多路复用层；keep-alive、chunked、body 流。
- Handler 走 `Service<Request,Response>`；`neton-http` 框架（路由/中间件/DI）重绑到 `neton-io-http`，**移除 `neton-http-hyper4k`（Rust FFI）依赖**。
- 与 msgtrans 长连接**共用同一 runtime / 同一 reactor / 同一 buffer 池**——这是相对 Ktor 的结构性优势。

替换判定（务实对冲）：`neton-io-http` 在同机 echo/req-resp benchmark 上**打平 hyper4k 的 ~70–80% 且稳定性达标**前，`neton-http` 继续保留 hyper4k；neton-io 先只承载 msgtrans 长连接。

---

## 9. 性能目标与度量

**北极星**：Go 的 `gnet` / 字节 `netpoll`（后者 RPC/长连接取向，与 msgtrans 场景最贴）一档；`ntex/geario`（Rust + io_uring）为参考天花板。

**度量口径（不玩虚的）：**
- echo 吞吐（QPS）、req/resp、p50/p99 延迟、最大连接数、每连接内存。
- 统一 harness：复用 `geario/bench-echo`，加 **Ktor-CIO、gnet、netpoll** 基线，同机同参。
- 阶段目标：
  - P1：`neton-io` polling 在 echo 上 ≥ `geario` polling 的合理分数（先立基线,不空喊）。
  - P3：io_uring 后端逼近 `geario`/gnet 一档。
  - HTTP：`neton-io-http` **超过 Ktor-CIO**、逼近 gnet/hyper4k。

**诚实前提与缓解**：KN 有 GC、无零成本抽象、协程有开销。ntex 的性能红利主要来自"少 syscall、少拷贝"的 buffered-filter 模型,忠实移植后有竞争力,但**要预留差距**。缓解：off-heap 池化 buffer、最小化热路径分配与装箱、epoll 边缘触发 drain、io_uring 批量提交、连接亲和无锁。**每阶段用数据说话,而非承诺。**

---

## 10. 依赖与被依赖

```
              neton-io（core/codec/net/rt/service/dispatcher/tls/server/http/testkit）
             /              |                 \
   msgtrans-kotlin     neton-io-http        （直接用户）
        |                   |
  Pulse                neton-http（去 hyper4k）
```
- `neton-io` **不依赖**任何上层（msgtrans/neton/pulse）。
- 平台后端通过 cinterop（liburing/openssl(官方)/epoll/kqueue）与 OS（Network.framework/IOCP/SChannel）——**这是不可避免的 C 边界,但远轻于内嵌 Tokio runtime**。

---

## 11. 里程碑

| 阶段 | 交付 | 验收 gate |
|------|------|-----------|
| **P0** | `bytes/codec/core(filter)/service/dispatcher` + **testing driver**;移植 geario 单测子集;echo dispatcher 纯 KN 跑通 | 模型单测全绿、任意 KN 目标可跑、零 cinterop |
| **P1** | `polling`（epoll+kqueue）+ 裸 TCP + connect/DNS;echo benchmark 立基线 | 对比 geario/Ktor 出数;稳定跑压测 |
| **P2** | TLS filter（OpenSSL 服务端 / nw Apple）+ WS codec;`msgtrans-kotlin` 落上来 | msgtrans conformance（与 Rust/TS wire 一致）通过 |
| **P3** | `io_uring` / `IOCP` / QUIC;`neton-io-http`（H1/H2） | io_uring 逼近 geario;http 超 Ktor-CIO |
| **then** | `neton-http` 重绑 neton-io-http、下线 hyper4k;Pulse 全量 | 全栈无 Rust runtime FFI |

---

## 11.1 平台与协议优先级（已定）

- **平台**：**Linux 优先（性能/benchmark 主场）+ macOS 作开发机（kqueue）**；iOS/Android 客户端走 `nw`/socket 后端，属 P2。多平台是最终要求（服务端 neton + 客户端 PulseKit 都要），但**性能攻坚只对 Linux 服务端**；客户端单连接、非吞吐战场，用 OS 原生栈即可。→ 底层借鉴偏 **gnet**。
- **协议**：**Raw TCP → WebSocket → HTTP**。第一个消费者是 `msgtrans-kotlin`（要 TCP+WS，不要 HTTP）；HTTP（替代 Ktor）是更后阶段。→ 参考偏 gnet/netpoll（底层高性能），**不是 nbio**（协议解析优化）。

## 11.2 Go 网络库借鉴与定位（gnet / netpoll）

geario 是**主蓝图**（架构 + filter/dispatcher/buffer 模型，已 benchmark）；gnet/netpoll 是**次要参考**——用于抄"GC 语言下 reactor 循环 + buffer 池化"的具体手法，不替代 geario。

| 借鉴点 | 来源 | neton-io 落法 |
|--------|------|--------------|
| Multi-Reactor（main accept + sub-reactor 亲和） | gnet | §3.3 |
| 分页/链式零拷贝 buffer | netpoll `LinkBuffer` ≈ geario `BytePages` | 从 geario 移植，native 内存 |
| epoll/kqueue/io_uring 直调 | gnet `internal/netpoll` | §6，cinterop |
| epoll→协程桥接（挂起/唤醒） | 自建 CoroutineDispatcher | §5，`resume` 在 reactor 线程 |
| 重业务卸载不阻塞 event-loop | gnet 协程池 | opt-in offload，默认不 hop |

> **修正 Gemini 两处**：(a) 不用 Ktor-IO 做 buffer（用 off-heap）；(b) read 的 continuation 默认 resume 在 reactor 线程、保持连接亲和，不是每次都 hop `Dispatchers.Default`。

## 12. 待定问题

1. JVM 后端是否要（当前 KN 优先，服务端 `macosArm64/linuxX64/linuxArm64/mingwX64` + 客户端 `iosArm64/iosSimulatorArm64`，`androidNative*` 排期待定）？
2. P0 内存 driver 之上,是否直接把 `msgtrans-kotlin` 的 wire codec 叠上跑 conformance（提前验证协议层）？
3. （已定）Linux TLS 用官方 OpenSSL，不用 BoringSSL。
4. io_uring 最低内核版本与 polling 回退策略细节。
5. buffer 池的所有权模型：是否完全照搬 geario 的 `BytePages`/`FilterBuf`,还是按 KN 的 pinned/native 内存重新设计？

---

## 附录 A：geario → neton-io 模块对照

`bytes→neton-io-bytes` · `codec→neton-io-codec` · `io→neton-io-core` · `service→neton-io-service` · `dispatcher→neton-io-dispatcher` · `net(+polling/uring/iocp/connect)→neton-io-net` · `tls/rustls→neton-io-tls(重造后端)` · `server→neton-io-server` · `rt→neton-io-rt` · `io/testing→neton-io-testkit`。

## 附录 B：参考

- `../geario`（Rust，ntex-io 提取；本 spec 的架构蓝本，含 `docs/benchmarks/`）
- ntex-rs / ntex-io（上游）
- Go：gnet、字节 netpoll（性能北极星）
- `../neton/neton-http-hyper4k` + `../hyper4k`（现有 Rust Tokio+Hyper 引擎,替换目标）

---

## 15. 实现现状与驱动选择（权威，随代码更新）

> 本节记录实际落地的架构与状态，覆盖前文里过时的分阶段设想。

### 15.1 统一 Reactor 抽象（readiness + completion 一套）

`neton-io-net` 里有一个内部抽象 `Reactor`（同时是 `CoroutineDispatcher`），把连接 I/O 暴露成
挂起操作：`read / write / accept / awaitConnect`。两类驱动实现它，上层（`ReactorStream` /
`Framed` / TCP 助手）对底层无感：

- **就绪型 `ReadinessReactor`**：等 fd 就绪再做 recv/send。后端 `Poller`：kqueue(Apple)、
  epoll、poll(Linux)。
- **完成型 `UringReactor`**：提交带 buffer 的 op，等完成拿结果（buffer 是模型的一部分）。

`ReactorStream` 是唯一的 `IoStream` 实现，把 read/write/close 委托给当前 `Reactor`。

### 15.2 驱动选择（默认 + 回退，无需配置）

`createReactor()` 按平台与 `NETON_IO_DRIVER` 选择：

| 平台 | 默认 | 可显式指定 |
|------|------|-----------|
| Linux | **io_uring**，内核不支持则**静默回退 epoll** | `NETON_IO_DRIVER=epoll \| polling \| iouring`（iouring 显式指定时不回退，创建失败即报错） |
| Apple | kqueue | `NETON_IO_DRIVER=polling`（poll(2)） |

即 Linux 生产环境**默认吃 io_uring**，老内核自动降 epoll，业务无需关心。

### 15.3 io_uring 实现要点

- K/N 的 Linux 交叉 sysroot 早于 io_uring（glibc 2.19 / kernel 4.9），故 UAPI struct + syscall
  由自带 cinterop `src/nativeInterop/cinterop/uring.def` **手写**；运行期由宿主内核提供。
- **单线程** submit/reap 围绕 `io_uring_enter`（本身是全屏障），**免 SMP ring 内存屏障**。
- **buffer 生命周期（完成模型的硬约束）**：op 提交到其 CQE 被回收之前，内核可能随时访问 buffer，故 buffer
  从提交起保持 `pin()`，**只在 reap 到该 op 的 CQE 时 unpin**。等待方协程被取消时不释放任何东西：只提交
  `IORING_OP_ASYNC_CANCEL` 并丢弃 continuation；原 op 的 CQE（真实结果或 `-ECANCELED`）恰好到达一次，由它
  完成回收。取消请求提交**不等于**原操作结束；cancel 与正常完成竞争时 cancel 自己的 CQE 报 `-ENOENT/-EALREADY`，
  忽略即可。reactor 退出时先取消全部在途 op 并 drain 到 CQE 全部到达再关 ring（关 ring 后内核是异步取消，
  不能先关）；若 drain 不收敛，剩余 buffer 保持 pin（泄漏并报告），绝不在内核手里释放。取消必须发生在 reactor 线程。
- **SQ 提交模型（正确性不依赖 ring 大小）**：`to_submit` 恒取 `sq_tail - sq_head`（直接读 ring），部分提交由下次 enter 自然补齐；`prepSqe` 写新 SQE 前若 ring 满则循环 `io_uring_enter` 提交腾位、CQ 满则先 `reap()`。ring 深度由 `NETON_IO_URING_DEPTH` 配置（默认 4096）。**饱和验证**：`NETON_IO_URING_DEPTH=4`/`8` 下跑 400 连接回归 `manyConcurrentConnections` 通过——4 项 ring 装不下 400 并发 op,证明满队列路径真的被走到且正确,而非"大到碰不到"。（此前 ~200 连接 stall 就是旧实现 depth=256 无溢出保护,benchmark 定位。）
- **socket 边界的失败路径**：非阻塞 connect 完成后读取 `SO_ERROR`，失败（拒绝/不可达）抛 `ConnectException`
  而不是交给上层一个"可写"但已死的 fd；向已断开的对端写入表现为 `EPIPE`/`ECONNRESET`（read 返回 -1、write 短写），
  而不是进程被 `SIGPIPE` 杀死；抑制在 socket/调用层（Apple `SO_NOSIGPIPE`，Linux `MSG_NOSIGNAL`，io_uring
  用 `IORING_OP_SEND`+`MSG_NOSIGNAL`），**不改进程信号处理**，宿主程序不受影响。回归测试 `TcpFailureTest`。
- **已知待验证项(诚实)**：①`io_uring_enter` 返回 EINTR/部分提交/EBUSY 目前统一走"循环重试 + 满则 reap",未按 errno 细分,需专项测;②取消/关闭期间的 buffer 生命周期已按上一条实现（pin 到 CQE、ASYNC_CANCEL、退出 drain），回归测试 `InFlightLifetimeTest`；**该实现只在 macOS 开发机上编译通过 linuxX64 并在 kqueue 上跑过同一测试，尚未在 Linux io_uring 上运行**，在 Linux CI（io_uring 变体）或 Linux 机器上跑通前不能判定验收通过；③多核扩展/尾延迟/内存表现未测——500 连接跑通只证明该负载可运行,不代表可扩展性已验证。

### 15.4 优化 backlog（交给贡献者；不阻塞上层业务）

当前 io_uring 是**功能正确的第一版**：每次 `enter` 只提交一个 op、每 op pin 一次 buffer，
ping-pong 下比 epoll 慢约 10%。io_uring 的性能红利在下列优化里，属底层完善项：

1. **批量提交**：一次 `io_uring_enter` 提交多个 SQE，摊薄 syscall。
2. **registered buffers**（`IORING_REGISTER_BUFFERS`）：免每 op pin/拷贝。
3. **SQPOLL**：内核轮询 SQ，稳态零 syscall。
4. **multishot accept / recv**：一次提交、多次完成。
5. **边缘触发 arm-once**（就绪路径）+ **多 reactor（每核一个）**：吃满多核。

benchmark harness 见 `neton-io-net` 的 echo server/client；对标 gnet/netpoll 与 geario。

### 15.5 平台/驱动矩阵现状

| 平台 | 驱动 | 状态 |
|------|------|------|
| macOS/iOS | kqueue | ✅ |
| Linux | epoll | ✅ |
| Linux | poll(2) | ✅ |
| Linux | io_uring | ✅（默认，naive，待优化见 15.4） |
| Windows | IOCP | ⏳ 未做（同 completion 模型，插同一 `Reactor` 抽象；需 winsock + mingw 目标 + Windows 测试环境） |

**实测验收（2026-09-13，可复现）**：环境 Rocky Linux 9.8 / 内核 5.14 / x86_64 / 4 核，io_uring 已启用；Kotlin 2.4.0、Gradle 8.14.2、JDK 17。命令 `NETON_IO_DRIVER=iouring|epoll|polling [NETON_IO_URING_DEPTH=8] ./gradlew :neton-io-net:linuxX64Test`。结果：`neton-io-net` 全部 13 个用例在 io_uring / epoll / poll(2) / io_uring(depth=8) 四种配置下**用例通过、0 失败**（depth=8 让 SQ 只容 8 项，收发+超时+取消并发时真正走满队列路径）。这只覆盖当前测试用例，不等于全部异常路径或可扩展性、尾延迟、内存表现已验证；后者仍未测。macOS(kqueue) 同样用例通过。区分：Windows 仅 core 模块编译通过，net 未做；跨语言 wire 互通（与 Rust/TS）尚未验证。

## 16. 多 reactor（thread-per-core）— 设计与验收

**依据**：2026-09-25 同机对照（docs/benchmarks/2026-09-25-geario-same-host.md）。12 连接下 geario 4 核 287–304k、单核 141–147k；neton 单 reactor 104–129k 且不随核数扩展。差距约 2× 来自核数、1.35× 来自每核就绪路径。多 reactor 是第一杠杆。

**模型（gnet Multi-Reactor / ntex server 同款）**
- N 个 reactor，每个独占一条 OS 线程（K/N `Worker`），各自一个 kqueue/epoll/io_uring 实例、任务队列、定时器。
- **reactor 0 为 acceptor**：持有监听 fd 并跑 accept 循环。接受到的 fd 按轮转 `i % N` 分发；目标为 0 则本地 `launch`，否则通过目标 reactor 的 `dispatch()`（已有的 MPSC 外部队列 + 自管道唤醒）投递一个任务，在**目标线程上**构造 `ReactorStream(fd, target)` 并 `launch` 连接处理。
- **连接终生钉在所属 reactor**：`ReactorStream` 的归属检查、msgtrans 的"连接状态单一所有者"契约都不变——所有者就是它被投递到的那个 reactor。热路径 share-nothing，无跨线程锁。
- 客户端 `connect()` 不变：在当前 reactor 上发起、归属当前 reactor。

**API**
```kotlin
/** 在 N 个 reactor 上服务一个 TCP 端点，阻塞直到停止。 */
fun serveTcp(host: String, port: Int, reactors: Int = cpuCount(), handler: suspend (IoStream) -> Unit)
```
`runReactor`/`listen`/`accept` 单 reactor API 保持不变。`echoServer` 第三个参数/`NETON_IO_REACTORS` 选 reactor 数。

**契约**
- 交接后 fd 只被目标 reactor 触碰；acceptor 不为其登记任何兴趣。
- 分发失败（目标已关闭）时 acceptor 关闭该 fd，不泄漏。
- 每个 reactor 的统计（`NETON_IO_STATS`）分别打印，带 reactor 序号。
- 关闭：`serveTcp` 由进程退出或显式停止结束；停止时先关监听，再让各 reactor root 完成并 `shutdown()`。

**验收**
- 正确性：`MultiReactorTest`——2 个 reactor、多条并发连接的 echo 全部正确；连接确实落在不同线程（各 reactor 处理计数 > 0）。macOS 与 Linux（153，三驱动）通过。
- 性能：153，`run-fair` 同口径，4 reactor 的 neton 在 12 连接下 ≥ 单 reactor 的 2×，并与 geario 4 worker 的数字并列记录。

## 17. 每核就绪路径：一次登记 + 边缘触发 + 就绪后再读 — 设计与验收

**依据**：钉到单核时 geario 141–147k vs neton 105k（1.35×）。`NETON_IO_STATS` 计数显示 neton 每请求 2 次 recv（其中 1 次必然 EAGAIN：写完立刻投机读）、每轮 poll 前重新登记兴趣（kqueue 一条 changelist / epoll 一次 `epoll_ctl`）。geario/ntex 的读路径是"兴趣常驻、就绪才读、读到 EAGAIN 为止"。

**改动（就绪型驱动）**
1. **兴趣常驻**：`ReactorStream` 创建时对 fd 登记一次读兴趣（kqueue `EV_CLEAR`、epoll `EPOLLET`），关闭时解除。不再每轮 one-shot 重登记。
2. **就绪标志**：每 fd 一个 `readable` 位。事件到来：若有 park 的读者则唤醒，否则置位。`read()`：先看位——置位则 recv 直到 EAGAIN 后清位；未置位则 park（不再投机 recv）。
3. **写侧**：先尝试 send；EAGAIN 才登记写兴趣（写兴趣仍 one-shot，写就绪事件稀少）。
4. 保持契约：`read` 语义（>0 / -1 EOF / 抛错）、close 唤醒、归属检查不变。
5. io_uring 驱动不受影响（完成型没有"就绪"概念；它的路径在 §15.4 批量提交）。

**验收**
- 单变量：仅此改动，其余不动。153 单核钉扎（run-fair3 口径）与 4 reactor 放开各跑 8 轮；`NETON_IO_STATS` 的 polls/req 下降。
- 正确性：全部现有测试 + 新增 `EdgeTriggeredTest`（半包/粘包、对端在 park 期间关闭、就绪位与 park 的竞争）。macOS + 153 三驱动。**已验收（2026-09-25）**：macOS 21/21；153 Linux epoll / io_uring / polling 各 22/22（含 MultiReactorTest、EdgeTriggeredTest）。

**结果（2026-09-25，153 第 5 轮，`docs/benchmarks/2026-09-25-geario-same-host.md`）**：polls/req 0.10（每次 `epoll_wait` 约 10 个事件），但吞吐仅 +2–4%（在抖动范围内）。原验收写的"reads_would_block/req 从 ~1 降到 ~0"是错的：第 2 条"recv 直到 EAGAIN"本身保证每个 burst 一次 EAGAIN，实测每请求恰好 1 次（1,525,923 recv / 762,578 请求）。这一次多余的 recv 就是与 geario 单核差距（1.27×）的全部来源，由 §17b 处理。

### 17b. 短读即排空（short-read rule）— 已试验，**否决**

**改动（已回退）**：`recv` 返回字节数小于提供空间即视为排空，清就绪位、不再做一次必然 EAGAIN 的 recv。

**结果（2026-09-25，153 第 6/6b 轮）**：单核钉扎 105k vs 103k（无差别，STATS 证实 EAGAIN recv 已归零：870,503 reads / 870,490 writes）；**4 reactor 放开 217k vs 328k（−33%，8/8 轮一致）**。原因由 6b 的每 reactor STATS 给出：§17 版本 667k 请求只用了 61k 次事件/唤醒——成功 recv 后就绪位保持，下一次 recv 直接读到下一个请求。这在 4 核跑 4 reactor + 12 客户端线程的过载场景下成立：`send()` 同步唤醒客户端线程并抢占 reactor，客户端在此期间收发完毕再阻塞，reactor 恢复后 recv 立即命中。短读规则每个请求都 park，退化为每请求一次 `epoll_wait`（STATS：polls ≈ 请求数）。这也解释了 4 reactor 下"物理上反常"的 ~10 µs p50（请求-应答在同核直接交接完成）。

**结论**：保留"读到 EAGAIN 为止"；那一次 EAGAIN recv 成本极低（单核数据无差别）而在争用下换来机会性批处理。就绪路径的 syscall 数不是单核差距的来源，下一步用 strace/perf 找真正的差距（见 §17c）。

### 17c. 单核差距的真实来源（perf，2026-09-25）— 设计与验收

**依据**（`docs/benchmarks/2026-09-25-geario-same-host.md` "Profiling"）：153 上 geario 默认就是 io_uring reactor（`COOP_TASKRUN|SINGLE_ISSUER|DEFER_TASKRUN`，批量 submit_and_wait），每请求约 0.27 次 syscall；neton epoll 约 2.95 次；neton io_uring 的 `io_uring_enter` 次数与 geario 相当。cpu-clock 采样（全部钉在 core 1）：geario 内核 93% / 用户 7%；neton epoll 内核 79% / 用户 12% / **GC 线程 7%**；neton uring 内核 68% / 用户 15% / **GC 线程 16%**。按每请求折算，neton uring 的内核时间已低于 geario；差距全部在 Kotlin/Native 侧：并发 GC 线程在同一核上 `sched_yield` 空转，以及用户态多出的 5–8%。

**实验顺序（每次一个变量，153 单核钉扎 12 连接 8 轮）**
1. GC 配置：(a) 固定目标堆 `NETON_IO_GC_TARGET_MB=64`（`GC.autotune=false`），(b) `-Xbinary=gc=stwms`（无独立 GC 线程，`echoServerStw` 二进制）。epoll 与 io_uring 各测。→ 第 7 轮。
2. 热路径零分配：`ReadOutcome` 对象、`HashMap<Int,…>`/`HashSet<Int>` 的装箱、`absorbExternal` 的 `ArrayList`、每次 dispatch 的 `Runnable`；目标是用户态占比接近 geario。
3. io_uring 驱动：setup 标志 `COOP_TASKRUN|SINGLE_ISSUER|DEFER_TASKRUN`；再考虑 multishot recv + provided buffers（内核已支持）。

**进展（2026-09-25）**
- 步骤 1 GC 配置：第 7 轮，≤ +9%，不采纳为库默认（库不设置进程级 GC 参数；`NETON_IO_GC_TARGET_MB` 仅为基准旋钮）。`gc=noop` 上限 ≈ 125k（第 7b 轮）。
- 步骤 2 零分配热路径：就绪路径 839be9e（fd 索引数组、Int 返回的 recv/send、按 fd 缓存 pin）第 8 轮 +9–14%；io_uring 67e0ce9（槽表 + 引用计数 pin）第 9 轮 +5.4%（成对 7/8）。第 8b 轮成对确认：neton/geario = 0.83（geario 安静时 ≈ 147k，neton ≈ 118k）。
- 步骤 3 去掉每次 park 的 `invokeOnCancellation` 节点（324f4e8）→ 第 10 轮。步骤 4 就绪回调内 `resumeUndispatched`（d69f6eb）→ 第 11 轮。步骤 5 io_uring 建环标志 `COOP_TASKRUN|SINGLE_ISSUER|DEFER_TASKRUN`（2bf194e，含回退与 `NETON_IO_URING_SETUP=legacy` A/B）→ 第 12 轮：单核成对 +3.9%（7/10），×4 +1.7%；保留。Linux 测试：排队运行中 `UringMappingLeakTest` 失败一次（映射数计数超过 slack），随后同一代码连跑 3 次 22/22 通过——记为一次未复现的失败，不当作已修复。
- 步骤 6 io_uring multishot recv + provided buffers（3f6d008，常量修正 6da57d3）→ 第 13 轮：单核成对 **+7.3%（8/10）**，×4 +6.7%（4/4）；保留。三驱动 Linux 测试 22/22。此后 uring 单核为 geario 的 0.88，epoll 0.85–0.92；uring ×4 仍为 epoll ×4 的 0.78。**§17c 单核验收（成对比值 ≥ 1.0）尚未达成。**
- 步骤 7 uring 热路径去掉取消回调节点（b0d32bb，仅 SEND 与 multishot 读者的 park；plain recv/accept/connect 仍保留内核取消）→ 第 14 轮：+3.1%（6/10），保留。四核：epoll ×4 成对 **1.245× geario（4/4）**。单核：uring 0.86、epoll 0.85–0.92。
- 方法修正：验收改为**与 geario 交替的成对轮次**（逐轮比值的中位数与胜场），单轮中位数对比不作数（153 主机一小时内漂移 ±30%）。

**验收**：钉扎单核 12 连接、与 geario 交替 ≥ 10 轮，逐轮比值中位数 ≥ 1.0（epoll 或 io_uring 任一驱动）；4 reactor 放开同法 ≥ geario（epoll 已达标：第 8/9 轮 ≈ 340k vs 294–300k）。
