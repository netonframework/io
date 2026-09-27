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
- 第 15 轮：GC 目标堆作为**服务端配置**（`NETON_IO_GC_TARGET_MB=64`，等价于 JVM 堆参数，不是库默认）：uring +6.5%（8/10），与 geario 成对 0.99（5/10）；同一二进制在第 14 轮为 0.86——成对比值在本机逐轮漂移约 ±5%，"单轮持平"只能读作"在噪声内持平"。
- 步骤 8 uring 先内联 `send(2)` 再回退 SQE（dde23b1）→ 第 16 轮 0.948（1/10），**否决并回退**（2ceb9d6）：多一次 syscall 比省下的 park 更贵，DEFER_TASKRUN 下 SEND SQE 本就在必经的 `io_uring_enter` 内执行。
- 第 17 轮：最终验收。~~**四核达标**：epoll ×4 成对 1.155× geario（6/6），311k vs 267k。~~ **撤回（2026-09-26）**：逐连接统计显示 epoll 驱动在多核负载下严重不公平（Jain 0.29–0.40，1000 连接时中位连接 8 秒只完成 3–4 个请求），该吞吐来自饿死多数连接，见 §19.5。**单核未达标**：最佳配置 io_uring + GC 目标堆 64 MiB 为 geario 的 0.949（20 轮中 6 胜），默认 GC 0.880（1/20），epoll + GC 64 为 0.895。剩余约 5% 在 Kotlin/Native 运行时（GC 线程 + 每请求的协程 park/resume），geario 无对应开销。
- 单核剩余手段（未做，按预期收益排序）：(a) 每连接复用 continuation、绕开 `suspendCancellableCoroutine` 的每次分配（需自写 Continuation 实现，风险高）；(b) 批量 CQE 的恢复合并；(c) 评估 `-Xbinary=gc=cms` 之外的 GC 调度参数。
- 方法修正：验收改为**与 geario 交替的成对轮次**（逐轮比值的中位数与胜场），单轮中位数对比不作数（153 主机一小时内漂移 ±30%）。

**验收**：钉扎单核 12 连接、与 geario 交替 ≥ 10 轮，逐轮比值中位数 ≥ 1.0（epoll 或 io_uring 任一驱动）；4 reactor 放开同法 ≥ geario（epoll 已达标：第 8/9 轮 ≈ 340k vs 294–300k）。

## 18. 全栈 Kotlin/Native 路线（2026-09-26 决定）

**规则（用户决定）**：所有底层核心——I/O 与多路复用、TLS 协议、HTTP/1.1、HTTP/2、WebSocket、DNS 协议——用
Kotlin/Native 实现；数据路径上**不得有外部运行时或引擎**（Tokio / hyper / geario 经 FFI）。**允许**对操作系统的薄调用
（syscall、libc 的 `getaddrinfo` 等）：neton-io 本身每次 recv/send 就是 cinterop 调用，这类调用单次成本低、无线程交接。
跨语言的真正损耗在于两个运行时并存：线程交接（Tokio 线程 ↔ Kotlin 线程）、缓冲区跨边界拷贝或 pin、回调往返。

**待决（见 18.5）**：密码学原语是否也必须用 Kotlin 实现。阶段 2（TLS）在此决定之前不开工。

### 18.0 阶段

| 阶段 | 内容 | 依赖 |
|---|---|---|
| 1 | msgtrans 多 reactor（18.1 + msgtrans SPEC §10）；地址：IPv6 + 域名解析（18.2）；压测矩阵（18.3） | — |
| 2 | TLS 1.3（再 1.2），包装 `IoStream` | 18.5 决定 |
| 3 | HTTP/1.1 服务端 + 客户端 codec；Neton `HttpAdapter` 基于 neton-io 的实现；与 hyper4k、geario-http 同接口同机压测 | 阶段 1 |
| 4 | WebSocket（服务端 + 客户端）；msgtrans `WebSocketClientTransport` 落地 | 阶段 3（升级握手） |
| 5 | HTTP/2（先 h2c，后 TLS + ALPN） | 阶段 2、3 |
| 6 | UDP；纯 Kotlin DNS 客户端；QUIC（远期） | — |
| 7 | Windows IOCP 驱动（有 Windows 部署需求时） | — |

每个阶段都带基准：与 geario / geario-http 同机、交替、成对比值（§17c 方法）。单核剩余约 5%（§17c 第 17 轮）暂缓。

### 18.1 公共多 reactor 服务 API（阶段 1）

§16 的 `ReactorGroup` 是 internal，`serveTcp` 会阻塞调用线程并自建 reactor，无法用在"已在 reactor 内"的调用方
（msgtrans `Transport.bind`）。新增：

```kotlin
/** 在当前 reactor 内调用：当前 reactor 为 0 号（负责 accept），另起 reactors-1 个工作 reactor。 */
suspend fun listenGroup(host: String, port: Int, reactors: Int = cpuCount()): TcpServerGroup

class TcpServerGroup {
    /** accept 循环；每个连接在其目标 reactor 上以新协程运行 handler。被 close() 结束时正常返回。 */
    suspend fun serve(handler: suspend (IoStream) -> Unit)
    /** 关闭监听；工作 reactor 在其上的连接协程全部结束后退出。只能在 0 号 reactor 上调用。 */
    fun close()
    val reactors: Int
}
```

契约：
- `reactors > 1` 时 handler 运行在与调用方不同的线程上；handler 拿到的 `IoStream` 以及它创建的一切归该 reactor 所有（§16 亲和性）。handler 访问共享状态时的线程安全由调用方负责。
- `reactors == 1` 时不起工作线程，等价于 `listen` + accept + 本地 `launch`。
- `serveTcp` 改为基于它实现，行为不变。

验收：`MultiReactorTest` 改走新 API 并保持通过；新增测试：`serve` 在 `close()` 后返回；handler 分布到 ≥ 2 个线程；三驱动 + macOS。

### 18.2 地址：IPv6 与域名解析（阶段 1）

- IPv6 字面量：`listen("::", port)`、`connect("::1", port)`；`listen("::")` 为双栈（`IPV6_V6ONLY=0`）。
- 域名：`connect(host, port)` 接受主机名。实现：`getaddrinfo` 在专用解析 Worker 线程上执行（薄 OS 调用，符合规则；它会阻塞，所以不能在 reactor 线程上调用），结果经目标 reactor 的 `dispatch` 送回；依次尝试各地址，全部失败才抛 `ConnectException`（Happy Eyeballs 以后再做）。
- 纯 Kotlin DNS 客户端需要 UDP，归阶段 6。
- 验收：`connect("localhost")`；IPv6 回环回显；无法解析的主机名得到 `ConnectException` 而不是崩溃；三驱动 + macOS。

### 18.3 压测矩阵（阶段 1）

回显之外的工作点，全部同机、交替、成对：
- 连接数 1 / 12 / 100 / 1000；载荷 128 B / 4 KB / 64 KB；
- 指标：qps、p50、p99、服务端 RSS（算出每连接内存）、CPU；
- msgtrans framed / rpc：1 reactor 对 4 reactor。

客户端：沿用 geario 的 `bench-echo/client`（每连接一个线程，1000 连接内可用）；1 万连接以上需要异步客户端，另行处理。
本节只记录数据、不设通过线，唯一硬指标是 msgtrans 4 reactor 在 12 连接及以上 ≥ 2× 单 reactor。

### 18.5 待决：密码学原语

TLS 分两层：协议层（握手状态机、记录层、密钥调度、X.509 证书链校验）和原语层（AES-GCM、ChaCha20-Poly1305、X25519、
SHA-256/384、HKDF、ECDSA P-256 / RSA 验签）。

- **协议层一定用 Kotlin 实现**（符合规则，也是工作量主体）。
- **原语层两种选择**：
  - (a) 纯 Kotlin：没有外部依赖。但 Kotlin/Native 用不了 AES-NI / PCLMULQDQ 这类指令，AES-GCM 预计比 libcrypto 的汇编实现慢数倍；密钥相关代码的常量时间性需要逐函数审计。
  - (b) 薄调用平台 libcrypto（Linux 用 OpenSSL libcrypto，Apple 用 CommonCrypto/Security）：一次调用处理一整条记录（≤16 KB），没有运行时、没有线程交接，按上面的规则属于允许的薄 OS/系统库调用。
- 建议 (b)，或 (b) 起步、之后按基准逐个替换成 Kotlin。**等用户决定。**

**评估 cryptography-kotlin（2026-09-26，用户提议）** — `dev.whyoleg.cryptography`，Apache-2.0，最新 0.6.0（Maven Central 2026-04-02），
用 Kotlin 2.3.20 构建（我们固定的 2.4.0 可以读取，已实际编译验证）；klib 覆盖 linuxX64/linuxArm64/macOS/iOS（另有 mingwX64）。
- **覆盖**：TLS 1.3 所需原语齐全——AES-GCM、ChaCha20-Poly1305、X25519/ECDH（P-256/384）、ECDSA、RSA-PSS、Ed25519、HMAC、HKDF、SHA-2；
  AEAD 支持显式 nonce（`encryptWithIvBlocking`，需 `@DelicateCryptographyApi`）；另有 ASN.1/DER、PEM 模块。
- **不包含**：TLS 协议本身（记录层、握手、密钥调度）与 X.509 证书链校验——这些无论如何都要我们写。
- **实现方式**：Native 上是 cinterop 薄调用 OpenSSL libcrypto（prebuilt 静态链接 OpenSSL 3.6.0，或 shared 链系统库）；Apple 另有 CommonCrypto / CryptoKit 提供者。没有额外运行时，符合 §18 规则。
- **热路径成本（实测，`bench/aead-bench`，同一份 OpenSSL）**：每条记录的 AEAD 加密，153 钉单核两次运行一致：

  | 记录大小 | 直接 libcrypto（每连接一个上下文） | cryptography-kotlin | 倍数 |
  |---|---|---|---|
  | 128 B | 297 ns | 4,638 ns | 15.6× |
  | 1 KB | 523 ns | 5,449 ns | 10.5× |
  | 16 KB | 4,454 ns | 15,764 ns | 3.5× |

  原因在 API 形态而不在 OpenSSL：每次调用都新建 `EVP_CIPHER_CTX` 并完整初始化密钥（AES 密钥扩展 + GHASH 表），再分配密文、tag
  与二者拼接三个数组。对 msgtrans 这类小包 RPC（每请求一条记录，服务端一次解密一次加密），约 9 µs/请求的密码学开销会让每核吞吐减半。
- **结论与建议**：**握手路径用 cryptography-kotlin**（密钥协商、签名/验签、密钥编解码、HMAC/HKDF/哈希——每连接一次，微秒级开销无所谓，
  且省下大量易错的密钥编码代码）；**记录层热路径自己写 libcrypto 薄调用**（每方向一个常驻 `EVP_CIPHER_CTX`，每条记录只换 nonce，直接在
  neton-io 的 `Buffer` 上原地加解密，零额外分配）。两者必须链接同一份 libcrypto：我们的 cinterop 只做声明、链接 provider 带的那份（已验证可行）。
  X.509 证书链校验自己写（可用它的 DER 模块解析）；iOS 客户端的信任评估可以直接用 Security.framework 的 `SecTrust`。


## 19. 工具链与运行时层的性能（2026-09-26）

**用户要求**：尽可能用 Kotlin/Native 最新特性换性能；只支持 Kotlin/Native，不为 JVM 妥协（三个仓库本来就只有 Native 目标）。

**核实的事实**（Maven Central 与官方发布说明）：
- 2.4.0（2026-06-03，当前使用）已带来 Native 的主要性能改动：CMS 并发标记成为默认 GC、LLVM 21、klib 编译期模块内内联。
- 2.4.10（2026-07-14）：发布说明中**没有** Native 后端、运行时或编译器的改动（Wasm、Compose、工具链修复）。
- 2.4.20（2026-09-07）为最新稳定版，包含 2.4.10；Native 改动是 Swift export 与 klib 增量编译（Beta），无运行时性能项。
- kotlinx-coroutines 1.11.0：无 Native 性能改动；`CoroutineDispatcher` 作为上下文键被弃用（neton-io 已用 `ContinuationInterceptor`）。

结论：升级工具链本身预计不带来运行时收益，但要升到 **2.4.20 + coroutines 1.11.0**（最新，并为之后的版本铺路），并按单变量测一次确认不退步。真正的杠杆是下面还没试过的编译/运行时选项，以及只做 Native 才能做的挂起路径改造。

### 19.1 工具链升级（单变量）
neton-io、msgtrans、pulsekit 同时升到 Kotlin 2.4.20、coroutines 1.11.0（复合构建必须同一版本）。验收：三仓库全部测试（macOS + 153 三驱动）；153 单核成对轮次 2.4.20 对 2.4.0 同一代码，比值应在 [0.97, 1.03] 内或更好。

**决定（2026-09-26，用户）**：Kotlin/Native 固定在 **2.4.0**，暂不升 2.4.10 / 2.4.20，以后有必要再议。以下为当时的分析。

**阻塞（2026-09-26）**：vip-application（本地 PulseKit 接入服务）以复合构建引入 neton-io、msgtrans，同时引入 Neton 框架仓库（`Neton/neton`、`geolite4k`、`hyper4k`），全部固定在 2.4.0。只升这三个仓库会让同一构建里出现两个 Kotlin 版本（两份 Kotlin Gradle 插件，或 2.4.0 编译器读 2.4.20 的 klib），通常无法构建。要升就得连 Neton 框架仓库一起升，超出 PulseKit 范围——**等用户决定**。由于 2.4.10/2.4.20 没有 Native 运行时改动，暂缓不影响性能工作；19.2、19.3 在 2.4.0 上进行。

### 19.2 编译/运行时选项 A/B（每次一个变量，单核钉扎成对 + 四核）
| 选项 | 依据 |
|---|---|
| `-Xbinary=gc=pmcs` | CMS 的并发标记线程在单核钉扎时与 reactor 抢同一个核（§17c 剖析：GC 线程 7–16%，大量 `sched_yield`）；PMCS 暂停更长，但总开销可能更低 |
| `-Xbinary=gcMarkSingleThreaded=true` | 并行标记的辅助线程在核不够时空转让出 |
| `-Xbinary=preCodegenInlineThreshold=40` | 代码生成前的 IR 内联（官方推荐值 40）；热路径是大量小函数与协程状态机 |
| `-Xklib-ir-inliner=full` | 跨模块内联（实验）：msgtrans → neton-io → kotlinx-coroutines 的调用都跨模块 |

胜出的选项写进 echoServer / 服务端二进制的构建配置；库不设置进程级 GC 参数（选项属于可执行文件，不属于库）。

### 19.3 挂起路径改造：去掉每次 park 的 `CancellableContinuation`（架构）
§17c 剖析里剩下的用户态成本是每次 park 的 `CancellableContinuationImpl` 分配、`installParentHandle`（JobNode）、以及 `DispatchedTask` 簿记。设计：
- park 用 `suspendCoroutineUninterceptedOrReturn` 直接保存协程自己的状态机续体，不分配 `CancellableContinuation`；
- 就绪时把续体放进 reactor 的续体队列（数组环形队列，不分配 `Runnable`），在 `drainTasks` 里直接 `resume`——仍在本线程、仍排队（不重蹈 §17c 步骤 4 内联恢复的覆辙）；
- 取消：每个流**一次性**登记所属 Job 的取消回调（`invokeOnCompletion(onCancelling = true)`），而不是每次 park 登记；取消时经 `postToReactor` 用 `CancellationException` 恢复该流上挂着的续体。
- 契约不变：close 唤醒 park、取消唤醒 park、归属检查、`InFlightLifetimeTest`。
- 先做就绪驱动，再做 io_uring；各自单变量成对测。

验收：全部测试（macOS + 153 三驱动 + msgtrans）；单核钉扎成对比值相对改造前 ≥ 1.03 才保留。

### 19.4 每连接内存：自适应读大小（2026-09-26）

**依据**：§18.3 矩阵中 1000 连接时 neton 每连接约 130 KB，geario 约 22 KB。库内原因：`ReactorStream` 每次读都
`reserve(64 KB)`，任何被读入的缓冲区（包括 msgtrans 所用 `Framed` 的读缓冲）首次读取就长到 64–128 KB 并终身持有。
对 PulseKit 接入（大量几乎空闲的 SDK 长连接）这是最主要的内存成本：1 万连接约 0.6–1.3 GB 空置缓冲。

**改动**：每个流自适应读大小（Netty `AdaptiveRecvByteBufAllocator` 的思路）：初始 2 KB；一次读填满了所给空间就翻倍；
连续两次读不到四分之一就减半；上限 64 KB、下限 2 KB。`reserve` 仍会用上缓冲区已有的全部空闲空间，所以这只限制不必要的增长。
io_uring multishot 路径本来按块精确 `reserve`，不受影响。基准 `echoServer` 同时改用默认初始容量的 `Buffer()`（不再预分配 64 KB），
与 geario 回显按需持有缓冲的做法一致——这是基准的变量，单独记录。

**验收**：全部测试；1000 连接 128 B 下每连接 RSS 显著下降；64 KB 载荷吞吐不低于改动前（成对）。

### 19.5 就绪驱动的公平性（2026-09-26，阻断级）

**事实**：`echo-client-fair` 测得 epoll ×4 在 100/1000 连接下 Jain 指数 0.39/0.29–0.32，1000 连接时中位连接 8 秒仅完成 3–4 个请求；io_uring ×4 与 geario 均为 0.95–1.00。此前所有"epoll ×4 胜过 geario"的数字因此撤回。

**机制**：`send()` 唤醒的客户端线程被调度到 reactor 所在核并立即发出下一个请求；reactor 的"读到 EAGAIN 为止"随即在同一连接上读到它并再次服务——少数连接对独占核（与第 6b 轮"一次唤醒服务约 11 个请求"一致）。

**改动**：每个连接每一轮只读一次。就绪 fd 上的读若本轮已成功读过一次，就把自己排到恢复队列末尾再读（不额外 `epoll_wait`、不分配），于是同一轮内所有就绪连接轮流被服务。

**验收**：`echo-client-fair` 在 100 / 1000 连接下 Jain ≥ 0.95（与 geario 同档）；在此前提下再比吞吐。单核钉扎也用公平客户端复核。**已达成（4340d6c，153）**：epoll ×4 Jain 0.989 / 0.996 / 0.990（12/100/1000 连接），单核 1.000。公平前提下 epoll 为 geario 的 0.87 / 0.96 / 0.96（×4）与 0.95（单核）；io_uring 0.76 / 0.91 / 0.88 与 0.88。

### 19.6 TCP_NODELAY（2026-09-26）

**事实**：neton-io 从未设置 `TCP_NODELAY`；geario 对每个流都设置。io_uring multishot（16 KB 缓冲）下 64 KB 回显被拆成四次写，每次写尾的不满段被 Nagle 扣住、等对端延迟 ACK，单连接只有 25 qps（p50 41 ms）。任何分多次写出的响应（包括 msgtrans 分段写出的包）都受影响。

**改动**：accept 与 connect 得到的 TCP 流默认设置 `TCP_NODELAY`（与 geario、Go 标准库一致）。

**验收**：io_uring multishot 64 KB 单连接恢复到与 epoll 同档；全部测试。**已达成**：25 → 11,172 qps（p50 86 µs）。

### 19.7 进度小结（2026-09-26，全部为公平客户端、成对轮次）

| 改动 | 结果 |
|---|---|
| §19.2 `preCodegenInlineThreshold=40`（服务端可执行文件） | 单核 +3.2%（9/12） |
| §19.3 就绪驱动去掉每次 park 的 `CancellableContinuation` | 单核 +6.6%（10/12） |
| §19.3 io_uring 同上 | 单核 +7.2%（9/12），×4 +6.7%（4/4） |
| §19.4 自适应读大小 | 1000 连接 RSS 134.6 → 15.9 MB（每连接 16 KB，低于 geario） |
| §19.5 公平服务 | epoll Jain 0.29–0.40 → 0.99；此前"多核领先"撤回 |
| §19.6 TCP_NODELAY | io_uring 64 KB 25 → 11,172 qps |

与 geario（成对中位数）：单核 epoll 0.964、io_uring 0.946；×4/100 连接 epoll 0.949、io_uring 0.965；×4/1000 连接 epoll 0.964（v20）。

**评估 openssl-kotlin 与 native-builds（2026-09-26，用户提议；用户要求性能优先）**

- **kio-labs/openssl-kotlin**（`io.github.kio-labs:openssl` 4.0.1.1，2026-07-12，Apache-2.0，单一维护者）：只有一个 `Placeholder.kt`，
  即对 `openssl/ssl.h`、`openssl/err.h` 的原始 cinterop，外加静态 `libssl.a`/`libcrypto.a`（**OpenSSL 4.0.1**），用 Kotlin 2.4.0 构建。
  没有封装损耗，但**只支持 linuxX64 与 macosArm64**（无 iOS、无 linuxArm64），发布历史仅两天、手工更新。**不作为基础**——它能给的，下一项都能给且覆盖更全。
- **ensody/native-builds**（`com.ensody.nativebuilds:*`，Apache-2.0）：用 vcpkg 自动构建最新版 C 库并按目标平台发布为普通 KMP 模块；
  OpenSSL 当前 **3.6.4（2026-08-28，与上游补丁版同步）**，覆盖我们全部目标（linuxX64/Arm64、macOS、iOS 真机与模拟器，另有 mingwX64、Android Native）。
  库模块只内嵌静态 `.a`（`libcrypto.a` 约 11 MB）、几乎不带绑定；头文件单独成模块，另有 Gradle 插件用于自写 cinterop。用 Kotlin 2.2.21 构建，2.4.0 可读。
  另有 zstd、zlib、brotli、lz4（压缩）与 curl、nghttp2/3、ngtcp2（协议实现，C 写的——与"核心用 Kotlin"的规则冲突，除非另行决定）。
- **关键验证（已实测）**：cryptography-kotlin 的 OpenSSL 提供者本身拆成 `openssl3-api`（只有声明）+ 链接模块，并已发布
  `openssl3-prebuilt-nativebuilds`（链接 native-builds 的 libcrypto）。在 Kotlin 2.4.0 上组合
  `cryptography-provider-openssl3-prebuilt-nativebuilds:0.6.0` + `nativebuilds:openssl-libcrypto:3.6.4` + 我们自己的仅声明 EVP 绑定：
  Gradle 把 3.6.1_1 统一解析为 3.6.4，二进制里只有**一份** OpenSSL 3.6.4，Linux/macOS 均可构建；153 上直接路径 289 ns/128 B 记录，与之前一致。

**建议的依赖结构（性能优先）**：
1. 二进制供给：native-builds（`openssl-libcrypto`，以后需要时 `zstd`/`zlib`/`brotli`）。安全更新随其自动发布跟进。
2. 记录层与所有热路径：neton-io 自己的仅声明 cinterop（只声明用到的 EVP 函数）+ 薄 Kotlin 封装（每方向常驻上下文、原地加解密）。
3. 握手：cryptography-kotlin（`openssl3-prebuilt-nativebuilds`），与第 2 条共用同一份 libcrypto。
4. 不需要复制 native-builds 的构建设施（vcpkg + Zig）；只有当它不提供某个库时才考虑自建。

## 20. 全平台（2026-09-26 用户决定：Kotlin/Native 支持的平台全部要支持）

> **最终范围（2026-09-26，用户）**：neton-io 只负责**高性能 I/O 与网络**；安全（TLS 等）由上层业务负责，不在 neton-io 范围内。
> 必须支持的平台：**macOS、Linux、Windows、iOS、Android**。目标：macosArm64/X64、linuxX64/Arm64、mingwX64、iosArm64/SimulatorArm64/X64、
> androidNativeArm64/Arm32/X64/X86。驱动：Linux 为 io_uring/epoll/poll，Android 为 epoll/poll（与 Linux 共用 epoll），Apple 为 kqueue/poll，Windows 为 IOCP（先 WSAPoll）。
> 源码集：`nativeMain` → `posixMain`（Linux + Android + Apple）→ `epollMain`（Linux + Android）/ `appleMain`；`linuxMain` 仅 io_uring；`mingwMain`（Winsock + IOCP）。
>
> **收窄（2026-09-26，用户，已被上一条取代）**：neton-io 必须支持 **macOS、Linux、Windows**，性能优先。现有的 iOS 目标保留（PulseKit iOS SDK 经 msgtrans 依赖它，
> 不增加工作量）；Android / tvOS / watchOS 不做。下面的 20 目标矩阵作为背景保留，已不是目标。实际目标：macosArm64、macosX64、linuxX64、linuxArm64、
> mingwX64，以及 iosArm64、iosSimulatorArm64、iosX64。源码集：`nativeMain` → `posixMain`（Linux + Apple）→ `linuxMain`（epoll + io_uring）/ `appleMain`；`mingwMain`（Winsock + IOCP）。

**目标矩阵（Kotlin/Native 2.4.0 实测）**：20 个目标——kotlinx-coroutines 1.10.2、native-builds OpenSSL 3.6.4、cryptography-kotlin 0.6.0 三者发布的集合完全一致：

| 平台族 | 目标 |
|---|---|
| Linux | linuxX64、linuxArm64 |
| macOS | macosArm64、macosX64 † |
| iOS | iosArm64、iosSimulatorArm64、iosX64 |
| tvOS | tvosArm64、tvosSimulatorArm64、tvosX64 † |
| watchOS | watchosArm64、watchosDeviceArm64、watchosSimulatorArm64、watchosArm32、watchosX64 † |
| Windows | mingwX64 |
| Android（Native） | androidNativeArm64、androidNativeArm32、androidNativeX64、androidNativeX86 |

† 2.4.0 中已标记弃用（"将在未来版本移除"），仍支持。**不含 linuxArm32Hfp**：2.4.0 已弃用，且 kotlinx-coroutines 与 OpenSSL 都不再为它发布。

**I/O 驱动**

| 平台族 | 默认 | 其它 |
|---|---|---|
| Linux | epoll | io_uring、poll(2) |
| Android | epoll | poll(2)；io_uring 启动时探测（应用沙箱的 seccomp 可能禁止），失败自动回退 |
| Apple（macOS/iOS/tvOS/watchOS） | kqueue | poll(2) |
| Windows | **IOCP**（完成型，同 io_uring 的抽象） | WSAPoll（就绪型，先用于打通正确性） |

watchOS 说明：Apple 对 watchOS 应用的底层 socket 有政策限制（仅特定场景允许）；库在 watchOS 上可编译、可在系统允许处运行。

**源码集层次**：`nativeMain`（反应器通用逻辑）→ `posixMain`（Linux + Android + Apple 的 POSIX socket）→ `epollMain`（Linux + Android）/ `appleMain`；`linuxMain` 仅放 io_uring；`mingwMain`（Winsock + IOCP）。
Windows 的 SOCKET 是句柄而非小整数 fd：Windows 驱动内部用自己的连接表（小整数 id → SOCKET），对上仍是 `Int`，不改反应器 API。

**验证矩阵**

| 目标 | 方式 |
|---|---|
| macosArm64 | 本机测试 |
| iosSimulatorArm64 | 本机 iOS 模拟器测试 |
| tvosSimulatorArm64、watchosSimulatorArm64 | 需要下载 Xcode 的 tvOS/watchOS 模拟器运行时（待用户同意） |
| linuxX64 | 153（三驱动） |
| linuxArm64 | 本机 colima/Docker 的 arm64 容器 |
| androidNativeArm64 | 本机 Android 模拟器 Pixel_9（arm64-v8a，API 36.1），adb 推送测试二进制运行 |
| mingwX64 | **需要 Windows 测试机或 CI**（待用户提供） |
| 其余架构变体（x64 模拟器、arm32、x86、device arm64） | 编译 + 链接 |

**阶段**：P1 构建矩阵（全部目标可编译链接）→ P2 Android / Linux arm64 / iOS 模拟器测试 → P3 Windows（先 WSAPoll 打通，再 IOCP 做性能）。msgtrans、pulsekit 随后跟进同一矩阵。

## 21. TLS 1.3（Kotlin 实现，性能优先；2026-09-26 用户选定方案 A）

> **移出 neton-io 范围（2026-09-26，用户）**：TLS 属于上层业务的安全职责，neton-io 不做。本节与 §18.5、§21.1 的评估保留作为资料，供上层将来参考。

**范围（第一版）**：只做 TLS 1.3；客户端 + 服务端；密码套件 TLS_AES_128_GCM_SHA256、TLS_AES_256_GCM_SHA384、TLS_CHACHA20_POLY1305_SHA256；
密钥交换组 x25519、secp256r1；签名 ecdsa_secp256r1_sha256、rsa_pss_rsae_sha256、ed25519（证书链中另需验 rsa_pkcs1_sha256）；SNI、ALPN、HelloRetryRequest、
KeyUpdate、告警与 close_notify。不做 0-RTT；PSK 会话恢复放第二版；不做 TLS 1.2。

**依赖（§18.5 已验证可共用一份 libcrypto）**：native-builds `openssl-libcrypto` 3.6.4；cryptography-kotlin `openssl3-prebuilt-nativebuilds` 0.6.0 用于握手。

**设计**
- **记录层（热路径）**：neton-io 自己的仅声明 EVP 绑定。每个方向一个常驻 `EVP_CIPHER_CTX`（密钥扩展只做一次），每条记录只设 nonce（`iv XOR seq`），
  在 neton-io `Buffer` 上原地加解密、零额外分配；写侧把一次 `write` 切成 ≤ 16 KB 的记录；读侧按记录头攒齐整条记录再解密。
- **握手**：状态机用 Kotlin 写；X25519/ECDH、签名/验签、公私钥编解码用 cryptography-kotlin；HKDF-Extract/Expand-Label 用 HMAC；握手转录哈希按需计算。
- **X.509**：DER 解析、链构建、签名校验、有效期、SAN 主机名匹配、KeyUsage/EKU；信任锚按平台取：Linux 系统 CA 包、Apple Security.framework（`SecTrust`）、
  Windows 证书存储、Android `/system/etc/security/cacerts`；另提供证书/公钥固定（自有服务端与 SDK 之间）。
- **API**：`TlsStream` 实现 `IoStream`，msgtrans 与以后的 HTTP 直接叠在上面；`connectTls(host, port, config)`、`listenTls(...)`。

**验收**：RFC 8448 测试向量；与 `openssl s_server/s_client`、Go `crypto/tls` 互通；性能与 geario + rustls 同机成对比较（TLS 回显，128 B / 16 KB，1/12/100 连接）；全部目标可编译，验证矩阵同 §20。

### 21.1 可行性与性能评估（2026-09-26，用户问"能否用 Kotlin Native 实现类似 rustls 的库"）

**结论：可以，而且架构与 rustls 同构。** rustls 本身不实现密码学：协议状态机用 Rust 写（sans-I/O），原语交给 provider（默认 aws-lc-rs，即 aws-lc 的 C/汇编）。
Kotlin 版是同样的分工：协议用 Kotlin，原语薄调用 libcrypto。性能取决于三块，均已在 153 单核实测（`docs/benchmarks/2026-09-26-tls-feasibility.md`）：

| | 估计（相对 rustls） | 依据 |
|---|---|---|
| 批量数据（16 KB 记录） | **0.95–1.0×** | 每条 16 KB 记录：aws-lc 4,390 ns，Kotlin→OpenSSL 4,435 ns；rustls 实测 3,328 MB/s |
| 小记录（128 B） | 每条多约 110 ns；RPC 场景约 −2.5% | aws-lc 176 ns，C→OpenSSL 220 ns，Kotlin→OpenSSL 285 ns |
| 完整握手 | **服务端约 0.7–0.75×、客户端约 0.85×** | 差距在原语：OpenSSL 的 X25519 比 aws-lc 慢约 2×，ECDSA 慢约 20%；rustls 服务端 8,931 次/秒 |
| 每连接内存 | 相当（估 5–20 KB；rustls 约 13 KB） | §19.4 的自适应缓冲 |

握手要追平，把原语后端从 OpenSSL 换成 aws-lc 即可（同样是 C、同样的薄调用层；native-builds 不提供，需要自己为我们的目标构建）。
设计上原语调用集中在一层内部接口之后，先用 OpenSSL，之后可换 aws-lc。

**质量与安全**：RFC 8448 测试向量；BoringSSL 的 BoGo 协议一致性测试套件（rustls 也在用，语言无关，通过 shim 程序接入）；与 OpenSSL、Go、rustls 互通；模糊测试。

## 22. Windows IOCP 驱动（设计，待有 Windows 测试机后实现）

WSAPoll 驱动（§20 P1）只用于打通正确性：它每轮 O(n)，且 Windows 上的 WSAPoll 有已知缺陷（例如连接失败时不报事件的旧行为），不适合作为性能驱动。
IOCP 是 Windows 上的完成型 I/O，与 io_uring 同属一类，沿用 `UringReactor` 的结构（槽表、原始续体、Int 结果环、取消语义）：

- **端口与关联**：每个 reactor 一个 `CreateIoCompletionPort`；流注册时把 SOCKET 关联到端口，并设 `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`
  ——操作同步成功时不再产生完成包，直接返回结果（快路径，省一次 `GetQueuedCompletionStatusEx`）。
- **读写**：`WSARecv` / `WSASend` 带 `OVERLAPPED`，缓冲区沿用按 fd 缓存的 pin（与 io_uring 相同，缓冲区在完成前保持 pin）；
  返回 `WSA_IO_PENDING` 时 park，完成包到达后经 Int 结果环恢复。`OVERLAPPED` 放在原生内存的槽里，完成包按其地址找回槽。
- **accept / connect**：`AcceptEx`、`ConnectEx`（经 `WSAIoctl(SIO_GET_EXTENSION_FUNCTION_POINTER)` 取得），完成后 `SO_UPDATE_ACCEPT_CONTEXT` /
  `SO_UPDATE_CONNECT_CONTEXT`。
- **唤醒**：跨线程 `dispatch` 用 `PostQueuedCompletionStatus`，不再需要回环套接字对；`Reactor` 的唤醒方式因此改为可覆盖。
- **定时器**：`GetQueuedCompletionStatusEx` 的超时参数；一次最多取一批完成包。
- **取消与关闭**：`CancelIoEx(socket, overlapped)`，缓冲区直到该操作的完成包到达才释放（与 io_uring 的规则相同）；关闭时先取消再 `closesocket`。
- **公平**：与 §19.5 同样的"每轮每连接一次"规则。

**验收**：全部测试在 Windows 上通过（IOCP 与 WSAPoll 两个驱动）；Windows 上与同机 Rust（tokio / geario 的 IOCP）回显成对比较，使用公平客户端。
**前提**：一台 Windows 测试机（或 CI 的 Windows 执行环境）。

## 23. 补齐与 geario 的 I/O / 网络功能差距（2026-09-26 用户确认：完成 1–7）

范围仍是"高性能 I/O 与网络"，不含 TLS。每项先实现、再测试（macOS 本机两驱动、Linux arm64 colima、Android 模拟器、iOS 模拟器、153 三驱动；Windows 走 CI），
涉及热路径的项在 153 上与改动前成对测量（公平客户端），性能不得退步（成对比值 ≥ 0.98）。所有新 API 在 `IoStream` 上以默认实现提供，
过滤层、测试流无需改动即可继续编译。

### 23.1 Windows IOCP 驱动（§22 设计）
`IocpReactor`（mingwMain），默认驱动；`NETON_IO_DRIVER=wsapoll` 回退。`Reactor` 的跨线程唤醒改为可覆盖（`wakeup()`），IOCP 用
`PostQueuedCompletionStatus`，唤醒管道改为惰性创建。验收：CI 上 Windows 的 IOCP 与 WSAPoll 两个任务全部测试通过。
**限制**：只能通过 CI 验证，结果需要仓库 API 访问（令牌问题待用户处理）或用户查看 Actions 页面。
**实现记录（2026-09-26）**：`IocpReactor` 已实现并推到 `ci/windows-validation`（C 垫片 `winshim.def` 封装 OVERLAPPED、AcceptEx、GetQueuedCompletionStatusEx，
Kotlin 只传基本类型）。与设计的差异：
- connect 未用 ConnectEx（需要重构 `tcpConnectAddr` 的发起方式），完成检测为零超时 WSAPoll + 1→10 ms 退避；建连时延最多多 10 ms，稳态收发不受影响。
- accept 先试一次非阻塞 `accept()`（积压中已有连接时不发重叠操作），否则 AcceptEx；AcceptEx 被拒（AF_UNIX）时退回轮询 `accept()`。
- 公平规则只约束"立即完成"（skip-on-success）的读：等完成包的读本来就跨轮，不再额外推迟。
- 本机无法运行（无 Windows、无 Wine），mingwX64 编译与测试二进制链接通过；**Windows 上的运行结果未验证**，需要查看 Actions 页面的 "Windows IOCP" 与 "Windows WSAPoll" 两个任务。

### 23.2 连接层：超时与反压
- **计时轮**（`Reactor` 内，与驱动无关）：分层级的哈希计时轮，精度 10 ms；流只记录截止时间，活动时只改字段（O(1)，无堆操作），轮到时再核对，过期才触发。
  时钟每轮循环读一次缓存，热路径不读时钟。
- **流级超时**（`IoStream.setTimeouts(TimeoutConfig)`，默认全关）：`readTimeout`（单次读挂起超过即抛 `TimeoutException`）、`writeTimeout`、
  `idleTimeout`（读写均无活动超过即关闭，相当于 keepalive）。触发时和取消一样唤醒挂起的操作（io_uring 同时发 `ASYNC_CANCEL`，缓冲区直到 CQE 才释放）。
- **连接超时**：`connect(..., options.connectTimeoutMillis)`，每个候选地址单独计时。
- **帧读取速率**（防慢速攻击，对应 ntex `frame_read_rate`）：`Framed` 在帧未收全时要求：从帧的首字节起 `timeout` 内至少有进展，每收到 `rate` 字节可延长，
  但总计不超过 `maxTimeout`；违反即 `TimeoutException`。
- **断开超时**：`IoStream.closeGracefully(timeoutMillis)` = 半关闭写端 → 读到 EOF 或超时 → 关闭。
- **写反压与合并**：`Framed.feed(item)` 只编码不写；写缓冲超过高水位（默认 64 KB）自动 flush；`flush()` 挂起直到内核接收，天然反压。
  `serve()` 改为"把已到的请求全部处理完、响应批量 flush 一次"，流水线请求不再一帧一次 syscall（顺序不变；单请求时延迟不变）。
- **读侧反压**：io_uring multishot 已收未读的数据超过上限（默认 256 KB/连接）时暂停重新挂 multishot，读空后恢复。
- 验收：各超时、帧速率、断开超时、反压的单元测试；153 上回显成对 ≥ 0.98（超时全关时热路径无额外成本）；流水线场景（每连接 16 个并发请求）批量 flush 的收益实测。

### 23.3 一次写多块与半关闭
- `IoStream.writev(buffers)`：POSIX 用 `sendmsg`（Linux 带 `MSG_NOSIGNAL`），io_uring 用 `IORING_OP_SENDMSG`，Windows 用多 `WSABUF` 的 `WSASend`；
  部分写入时按实际字节推进各缓冲区。默认实现逐个 `write`。
- `IoStream.shutdownOutput()`：`shutdown(SHUT_WR)` / `SD_SEND`，对端读到 EOF，本端仍可读。
- 验收：多块写的边界测试（部分写、空块、大量小块）；半关闭后对端 EOF 而本端继续读；`Framed` 批量 flush 使用 writev 的实测。

### 23.4 服务端
- `TcpServerGroup` 增加：`maxConnections`（全组上限，到达后暂停 accept，连接结束后恢复）、`pause()` / `resume()` 接收、
  `shutdown(gracefulTimeoutMillis)`（停止接收 → 等活动连接结束或超时 → 取消剩余连接）、`activeConnections`。
  连接协程挂在组内专用的 `SupervisorJob` 下，便于统一取消。
- **SO_REUSEPORT 接收模式**（Linux）：每个 reactor 各自一个监听 socket、各自 accept，内核分发，无跨线程移交。`acceptMode = Handoff | ReusePort`，
  153 上成对测量吞吐与公平性，胜出者作为 Linux 默认。
- 验收：上限、暂停/恢复、平滑停机的测试；ReusePort 的吞吐与 Jain 指数。
- **实现记录**：连接协程直接挂在各 reactor 的作用域下，组内按 reactor 记录连接 Job（仅在本线程访问）用于停机时取消，没有另建 SupervisorJob。
  接收槽位在 accept 之前预留（多个 ReusePort 接收循环不会超过上限），连接结束时在其所在线程释放。`close()` 会唤醒停在上限或暂停闸门上的接收循环；
  已关闭的监听器拒绝 accept（旧实现对已关闭的 fd 号 accept 会永久挂起）。
  153 成对结果（x4，6 轮）：ReusePort / Handoff 在 100 与 1000 连接下吞吐比 0.98–1.02、Jain 相当，**无胜出者，Linux 默认保持 Handoff**；ReusePort 作为选项保留。

### 23.5 套接字选项
`SocketOptions(noDelay = true, keepAlive = null | KeepAlive(idleSec, intervalSec, count), sendBufferSize, receiveBufferSize, backlog = 1024,
reuseAddress = true, reusePort = false, lingerSec = null, connectTimeoutMillis = 0)`，用于 `listen`、`listenGroup`、`connect`；监听端的选项同样施加到接受的连接上。
平台差异在各自实现中处理（macOS 的 `TCP_KEEPALIVE` 对应 Linux 的 `TCP_KEEPIDLE`；Windows 10 起支持 `TCP_KEEPIDLE/KEEPINTVL/KEEPCNT`；Windows 不设 `SO_REUSEADDR`）。
验收：每个选项设置后用 `getsockopt` 读回核对。

### 23.6 Unix 域套接字
`listenUnix(path, options)`、`connectUnix(path)`，以及 `listenGroup` 的 Unix 版本；Linux / Android / Apple 与 Windows（10 1803+ 的 AF_UNIX）。
`sockaddr_un` 按平台布局构造（Apple 有 `sun_len`）；Linux 支持抽象命名空间（路径以 `@` 开头）。监听前若路径是无人监听的旧 socket 文件则删除。
验收：本机回显、路径过长报错、旧 socket 文件处理；153 上与 TCP 回环的吞吐对比。
**实现记录**：复用 TCP 的 listen / connect / accept 路径，只有 `sockaddr_un` 布局与错误码按平台区分。旧文件处理改为"先 bind，失败才探测"：
探测连接若打到仍在监听的进程，会出现在对方的 accept 队列里，所以只在 bind 已失败时才探测。Android 的 adb shell（SELinux）不允许在 /data/local/tmp 创建 socket 文件，
相关测试在该环境跳过；抽象命名空间在 Android 上通过。

### 23.7 缓冲池与零拷贝
- **`BufferPool`**：每线程一个（无锁），按 2 KB–64 KB 的 2 的幂分级缓存数组，每级数量与总字节有上限。
- **池化 `Buffer`**：数组在首次写入时才向池申请，读空时归还——空闲连接不占缓冲；增长时换更大一级、归还旧的。`Io` 的读写缓冲默认池化。
- **`Bytes`**：不可变零拷贝切片（数组 + 偏移 + 长度），`Buffer.readSlice(n)` 不拷贝；被切片引用的数组标记为共享，之后 `Buffer` 需要整理或复用时改为换新数组（写时复制），
  共享数组不再回池，由 GC 回收——切片永不会被改写。`writev` 接受 `Bytes`。
- 验收：池化后 1000 个空闲连接的内存；切片在后续读写、整理后内容不变的测试；153 上回显成对 ≥ 0.98。
- **实现记录**：`clear()` 不归还数组（驱动可能仍持有该数组的 pin），只有读空时归还；readiness 驱动在挂起读之前归还空的池化缓冲。
  io_uring 单发读在挂起期间把数组交给内核，不能归还（multishot 模式不占用）。空缓冲的 iovec 用空指针（池化缓冲读空后持有 0 长度数组）。

### 执行顺序
23.5 → 23.3 → 23.2 → 23.4 → 23.6 → 23.7 → 23.1（IOCP 代码随时推 CI，验证取决于能否读取 CI 结果）。

## 24. Linux 吞吐第一：零分配收发路径（2026-09-27 用户确认：Linux 优先，先把吞吐做到第一，macOS / Windows 之后再优化）

### 24.1 测量依据（153，单核绑定，12 连接，128 B；raw：`docs/benchmarks/2026-09-27-153-p0/p1/p2-raw.txt`）
- 每请求 CPU 约 9.7 µs，其中内核态约 8.8 µs；neton 用户态约 840 ns，geario 约 600 ns。
- 系统调用/请求：neton epoll 3.1、io_uring 1.5，geario（默认 io_uring）0.29。
- 其中 `sched_yield` 每请求 0.84（epoll）/ 1.13（io_uring）次，gdb 调用栈全部来自 Kotlin/Native 的 `MainGCThread::PerformFullGC`：
  GC 线程等待反应器到达安全点时自旋让出。GC 触发频繁，源于每个请求在堆上分配约 3 个协程续体
  （`ReactorStream.read`、`ReadinessReactor.read`、`ReadinessReactor.write` 的状态机）以及 Int 结果装箱。
- 同一份代码的 GC 变体（6 轮成对，对 geario）：默认 epoll 0.87 / io_uring 0.98；关闭 GC epoll 0.97 / io_uring 1.01（未带内联选项，偏保守）；
  固定 128 MB 目标堆只到 0.92 / 0.99；STW 更差。**结论：去掉每请求的堆分配就是主要的吞吐杠杆。**

### 24.2 设计
- **尾调用链**：`ReactorStream.read/write` → `Reactor.read/write` → 挂起原语，全部是尾调用，不生成状态机；挂起时交给反应器的是调用方
  （如连接处理循环）自己的续体，一个连接只在其处理协程启动时分配一次。
- **反应器完成读写**：读先尝试一次非挂起的 recv（快路径）；需要等待时把 `(续体, 目标缓冲, 读大小策略)` 登记在按 fd 的槽里，
  就绪事件到来（或公平规则推迟到下一轮）时由反应器执行 recv，再以结果恢复续体。写同理：先非挂起地发送，剩余部分登记后由可写事件续发，
  全部发完再恢复。io_uring：multishot 读由 CQE 到达时直接拷入登记的缓冲；SEND 在 CQE 里续发剩余部分，发完才恢复。
- **自适应读大小**移到接口 `ReadSizer`（`ReactorStream` 实现，反应器在读成功时回调），不再需要调用返回后的处理。
- **结果不装箱**：恢复续体时使用预装箱的 Int 缓存（0..65536），更大的值才按常规装箱。
- 公平规则（§19.5）、取消、超时（§23.2）、关闭语义保持不变；带超时的路径仍可走状态机（非默认）。
- 暂不改动：io_uring 非 multishot 读、`writev`、accept / connect、IOCP（Windows 之后再做）。

### 24.3 验收
- 153 单核绑定 12 连接回显：`sched_yield`/请求 ≤ 0.05；cachegrind 下每请求 `CustomAllocator::Allocate` 调用为 0（稳态）。
- 吞吐：单核绑定与 ×4 100 连接，epoll 与 io_uring 对 geario 成对中位数 ≥ 1.00。
- 用户态指令/请求不高于 v34；全部测试通过（Linux 三驱动、macOS 两驱动、msgtrans）。

### 24.4 结果（2026-09-27，详见 `docs/benchmarks/2026-09-27-zero-alloc.md`）
- 稳态每请求堆分配 0、`sched_yield` 0（v38）；用户态指令 epoll 2325 / io_uring 2842（v34 为 3391 / 4561，geario 约 4050）；io_uring 每请求系统调用 0.094（geario 0.29）。
- 途中修复：readiness 驱动 EOF 与 WOULD_BLOCK 同为 -1 的冲突；计时轮首次扫描起点错误（截止时间可能被推迟一整圈 5.12 s，v30 引入）。
- 对 geario：**io_uring（Linux 默认）在所有测量点持平或领先**（单核 1.022 八轮全胜，×4 1000 连接 1.005，×4 100 连接剖析三轮全胜）。
  epoll 0.96–0.99，未达验收；短读规则单核领先（1.051）、四核落后，下一步改为按连接自适应。


### 24.5 epoll：按连接自适应的短读规则
- 背景：边沿触发下，一次读到的数据少于请求量（短读）后，下一次 recv 是"投机"的：单核时对端还没回，多半返回 EAGAIN，白白一次系统调用；
  四核时对端（另一核上的客户端）常已发来下一请求，投机 recv 直接命中，省掉一轮 epoll_wait。固定开或关各输一种场景（§24.4）。
- 规则：每个连接记住最近一次投机 recv 的结果。落空（EAGAIN）则之后短读就不再投机，直接等下一个边沿；每 16 次短读仍投机一次，重新学习。
  命中则保持投机。收到对端关闭（HUP/RDHUP）后不跳过（FIN 可能已随数据到达，不会再有边沿）。
- **修订（v39 实测）**：按连接决策在 ×4、1000 连接下造成不公平（p99 17–21 ms 对 11 ms，Jain 0.87–0.95 对 0.98–0.99）：
  投机的连接每轮开头就被重试，等边沿的连接要经过每次最多 64 个事件的 epoll_wait，同一反应器里混用两种路径等于插队。
  改为**按反应器**决策：最近 256 次投机 recv 落空过半则整个反应器跳过投机（仍每 16 次短读抽样一次），同一反应器内策略一致。
- `NETON_IO_SHORT_READ=0`（从不跳过）/ `1`（总是跳过）/ 默认 `auto`。验收：153 上 auto 对 off 与 always 成对，单核与 ×4（100、1000 连接）都不劣于两者中较好的一方
  （差值在噪声内），全部测试在三种模式下通过。
- **修订 2（v40 实测）**：按反应器决策恢复了公平（1000 连接 p99 9.7 ms、Jain 0.968），但 ×4 吞吐低于 off（100 连接 0.911 对 0.963，1000 连接 0.959 对 1.045）；
  单核与 always 持平（1.021）。推测 ×4 下落空率约一半，反应器在两种模式间摇摆。门槛改为落空率 > 90 % 才跳过；若 v41 仍不满足验收，默认改回 off，auto 仅作选项。
- **结果（v41，门槛 90 %；raw `docs/benchmarks/2026-09-27-153-v41-raw.txt`）**：按中位数 auto 在每个区间都取到较好的一方——单核 126.6k（always 125.7k、off 112.0k，
  系统调用 2.17/请求，已进入跳过模式）；×4 100 连接 296k（off 302k，差在噪声内，系统调用同为 2.05，保持投机）；×4 1000 连接 234k（off 221k），Jain ≥ 0.976。
  验收通过，auto 为默认。当前 epoll 对 geario：单核持平（中位数 126.6k 对 127.3k），×4 100 连接 0.93–0.95，×4 1000 连接 1.06–1.08。


### 24.6 GC 调优接口（不默认启用）
neton-io 自身收发路径已零分配，但上层协议每条消息仍需分配（解码对象、载荷、响应）。Kotlin/Native 的 GC 会暂停所有线程，其协调线程自旋等待安全点，
每次回收都占用反应器时间。`GcTuning.fixTargetHeap(mb)` / `GcTuning.fromEnvironment()`（`NETON_IO_GC_TARGET_MB`）固定目标堆、关闭自动调节，
让回收更少发生。这是进程级设置，库不默认修改，由服务端应用在启动时决定。
实测（153，单反应器）：msgtrans rpc 在 64 / 256 MB 目标下吞吐 +5.5 % / +5.7 %，framed 256 MB +4.6 %，每请求 GC 自旋减半
（`msgtrans-kotlin/bench/results/2026-09-27-153-mt4-raw.txt`）。

### 24.7 io_uring 读：直接收进用户缓冲（单次 RECV）取代 multishot 默认
- 依据（153，v42，raw `docs/benchmarks/2026-09-27-153-matrix{2,3}-raw.txt`）：吞吐矩阵中 io_uring 只在 64 KB 输给 geario（0.80–0.89，全部轮次）；
  multishot 用 16 KB 内核缓冲池，每条 64 KB 消息拆成 ≥ 4 个 CQE、每块一次拷贝与一次归还。对照（对 geario）：
  单次 RECV 进用户缓冲 64 KB 单核 1.656、×4 1.480，4 KB 持平（≈ 1.1），128 B ×4 1.017、单核 0.966；常驻内存 ×4 由 46 MB 降到 20 MB（无缓冲池）。
  geario 的 io_uring 同样是单次 Recv 进自身缓冲。单次路径当时仍是状态机（每次读分配一个续体），128 B 单核的差距即来自此。
- 设计：单次读零分配——SQE 指向用户缓冲（已 pin），槽里登记缓冲与读大小策略，CQE 到达时由反应器提交数据（commitWrite + onRead）再恢复调用方；
  读被取消/超时则 ASYNC_CANCEL，CQE 中已收到的字节照常提交进缓冲（不丢数据），再以原因恢复；流被关闭则放弃缓冲、不再触碰。
- 默认改为单次读；`NETON_IO_URING_MULTISHOT=1` 保留 multishot（空闲连接不占读缓冲时更省内存）。
- 验收：153 上单次（零分配）对 multishot 与 geario，128 B / 4 KB / 64 KB × 单核 / ×4 100 连接，单次在各点不劣于两者较好者（噪声内）；
  每请求分配 0；三驱动全部测试与 msgtrans 测试通过。
- **公平性（v43–v44，raw `docs/benchmarks/2026-09-27-153-{fair64,v44-pf}-raw.txt`）**：64 KB / 1000 连接（服务端与客户端分核）下，单次 RECV
  约 10 % 的连接 8 s 内只完成 1 个请求（p10 = 1，Jain 0.54–0.79）；快照显示这些连接的 socket 里已有完整请求未读。关闭 DEFER_TASKRUN 同样饿死，
  去掉每轮任务预算或加 `IORING_RECVSEND_POLL_FIRST` 均可消除；POLL_FIRST 吞吐与不加持平（0.96–1.03）、64 KB 略优。
  机制：不加 POLL_FIRST 时数据已到的 RECV 在提交时当场完成，忙碌连接反复走这条路径，先前挂起的 RECV 迟迟不完成（内核层面的确切原因未查明）。
  **单次 RECV 默认带 POLL_FIRST**（geario 相同），`NETON_IO_URING_POLL_FIRST=0` 关闭。


### 24.8 io_uring 注册文件（实验，默认关闭）
`NETON_IO_URING_FIXED_FILES=1`：启动时登记稀疏文件表，流的 socket 放入与 fd 同号的槽，RECV/SEND/SENDMSG 用 `IOSQE_FIXED_FILE`。
陷阱：文件表在更新后仍持有旧文件，直到更新前提交的请求全部完成，单纯 close() 不会发出 FIN（对端挂起的读永远等待，EdgeTriggeredTest 发现），
因此已登记的 socket 关闭前先 `shutdown(SHUT_RDWR)`。
结果（153，6 轮，raw `docs/benchmarks/2026-09-27-153-v46-fixedfiles-raw.txt`）：对默认 io_uring 0.989–1.025，全部在噪声内——无可测收益，保持默认关闭。
同轮 128 B ×4 100 连接 io_uring 对 geario 0.935（0/6），历次 0.93–1.00：此处为稳定的小幅落后；同场景每请求 CPU 低于 geario（§24.4 p4 剖析），
推测与客户端同机争用 CPU 的调度有关，尚未定位到服务端可改之处。

### 24.9 Linux 阶段结论（2026-09-27）
- 128 B 的平局是物理下限：服务端 0–1 号核、客户端 2–3 号核分开时，io_uring 与 geario 每请求服务端 CPU 相同（7.1–7.8 µs 对 7.5–7.8 µs），
  吞吐 0.98–1.04；其中用户态约 1 µs，其余是回环 TCP 协议栈，两边一样（raw `docs/benchmarks/2026-09-27-153-sep128-raw.txt`）。
- 4 KB 与 64 KB 在所有连接数下领先（io_uring 1.12–1.78，epoll 1.08–1.68，4/4 轮），1000 连接的 128 B 领先 1.10；io_uring 每请求系统调用约为 geario 的一半；
  零分配、无 GC 自旋；64 KB 千连接无饿死（Jain 0.94–0.96，geario 0.999）。
- 下一阶段：macOS（kqueue），随后 Windows。

## 25. macOS（2026-09-27）
- 基线（本机 10 核、共享、负载 5–10；raw `docs/benchmarks/2026-09-27-mac-matrix-raw.txt`）：neton kqueue（10 反应器）对 geario（10 工作线程）
  64 KB 1.58–1.76（4/4），128 B / 4 KB 两者被客户端封顶在同一吞吐（≈ 1.00）；每请求服务端 CPU kqueue 少 10–13 %（13.5–14.1 µs 对 15.0–16.2 µs）。
- 途中修复：macOS `nc -z` 以 RST 断开，读错误逃出连接协程，取消整个反应器作用域（单反应器时整个服务端退出，多反应器时随后因写已关闭的唤醒管道死于 SIGPIPE）。
  现在连接处理函数的失败只结束该连接（stderr 记一行）；反应器关闭唤醒管道前先停止写入；Apple 的管道设 F_SETNOSIGPIPE。

### 24.10 Buffer 整数原语
依据（153，callgrind，msgtrans framed / rpc，epoll 单反应器 12 连接 64 B；每请求 6006 / 7921 条指令）：`Buffer.ensureWritable` 777 与 `Buffer.getByte` 640 条/请求——
16 字节包头逐字节写读，每字节一次函数调用和完整检查。新增大端序 `writeShort / writeInt / writeLong`、`getUnsignedByte / getUnsignedShort / getInt`、
`readInt / readUnsignedShort`，每个值一次边界检查；`writeByte` 有内联快路径。msgtrans 编解码改用这些原语。

### 24.11 同反应器交接：ReactorResumer
协议层在同一反应器的协程之间交接（如 msgtrans 读循环 → 处理循环）时，kotlinx 的 `intercepted().resume()` 要经过调度器、`DispatchedTask`
与上下文查找（callgrind：rpc 每请求约 300–400 条指令）。反应器新增对象值恢复环；`reactorResumer(context)` 取得一次，`resume(cont, value)`
在反应器线程上只占一个环槽，其它线程调用时回退到调度恢复。

### 24.12 取消监听只在真正取消时工作
`invokeOnCompletion(onCancelling = true)` 的回调在作业**正常结束**时也会被调用（cause 为 null）。各驱动与 msgtrans `ReactorQueue` 的回调
不看 cause，一律投递"作业被取消"处理并调用 `job.getCancellationException()`——每次都新建一个带栈回溯的异常。在短命作业里挂起时
（msgtrans `request()` 的 `withContext`，INLINE 写模式下发请求的协程自己写 socket），这发生在每个请求上：153 上 rpc 客户端
`_Unwind_Find_FDE` 占 7.9 % CPU，每请求 CPU 27 µs 对 CHANNEL 的 12 µs，吞吐 4 万对 8–10 万。现在只在 cause 非空时处理。

## 26. 高并发（2026-09-27 用户："不要炫技，要科学的使用 Kotlin Native 去实现高性能，比如协程 + gc 优化，让网络库高并发无敌"）

### 26.1 原则
- 并发模型就是 Kotlin 协程：一个连接一个协程，挂在反应器上；不引入自造调度器、不绕开 `CancellableContinuation`。
- 性能来自两处，都用 Kotlin/Native 自身的机制：协程挂起/恢复不分配（§24 的规则），GC 次数与停顿可测、可调（`GC` 运行时 API）。
- 每项优化先测后改：每请求分配数 / 指令数（callgrind）、153 配对吞吐、GC 统计三者之一给出收益才保留；没有收益的改动回退，并在此记录。
- 自造的数据结构只在热路径、且测得出收益时使用。

### 26.2 场景（153，4 vCPU；服务端 `taskset 0,1` 两个反应器，客户端 `taskset 2,3`；对照 geario 同核同线程）
- **C1 全活跃**：N ∈ {1k, 10k, 50k} 连接，每连接一个请求在途（闭环），128 B。指标：吞吐、p50/p99/p999、按连接计数的 Jain 指数、服务端 RSS。
- **C2 空闲 + 活跃**：N ∈ {0, 10k, 50k} 条空闲连接之外，64 条活跃连接。指标：活跃吞吐相对 N=0 的比值（空闲连接是否拖慢活跃连接）；
  每连接内存 `(RSS_N − RSS_0) / N`。
- 每格配对轮换 ≥ 4 轮。

### 26.3 工具
- 客户端 `bench/echo-client-mass`（Rust，mio，与被测库无关）：T 个线程各自一个 epoll，连接平均分配；源地址在 127.0.0.1–127.0.0.8
  之间轮换，绕开单个目的地址约 2.8 万个临时端口的上限；连接全部建立后才开始计时。
- 服务端 GC 统计：`NETON_IO_GC_STATS=1` 时 echoServer 每秒打印一行，取自 `kotlin.native.runtime.GC.lastGCInfo`：GC 次数（epoch 差，精确）、
  采样到的停顿总和与最大值、GC 后堆大小。采样间隔 2 ms，一次采样之间发生多次 GC 时只记得到最后一次的停顿，报告中注明采样数。

### 26.4 首轮结果（153，v47，4 轮均值；raw `docs/benchmarks/2026-09-27-153-hc1-raw.txt`）
C1 全活跃，neton / geario 吞吐比与服务端峰值 RSS：

| 连接 | epoll | io_uring | geario req/s | RSS neton / geario |
|---|---|---|---|---|
| 1k | 1.14 | 1.05 | 297k | 22 / 24 MB |
| 10k | 1.04 | 1.05 | 238k | 53 / 186 MB |
| 50k | 1.05 | 1.04 | 188k | 184 / 509 MB |

p99 / p999 与 geario 持平或更低；Jain ≥ 0.97。C2（64 活跃 + N 空闲）活跃吞吐：epoll 417k / 419k / 413k（N = 0 / 10k / 50k），
geario 412k / 440k / 361k（50k 空闲时 0.88）；每条空闲连接内存 epoll 1.5 KB、io_uring 3.4 KB、geario 6.1 KB。

GC：稳态无 GC（1k 连接 8 s 约 250 万请求，0 次）；GC 只在建立连接、堆增长时发生，50k 连接全程 5–6 次、停顿合计约 1 ms、单次 ≤ 0.6 ms。
到达安全点时间的最大值在所有格都是约 6 ms——CFS 一个调度周期：两个核上两个反应器加 GC 线程，被抢占的反应器要等下一个时间片才能到达安全点；
属于核超额分配，不是库内的长循环。

待解决（按数据）：
1. 建立连接慢：10k 连接 epoll 0.42 s、io_uring 1.72 s，geario 0.15 s；50k 为 2.4 / 3.2 s 对 1.3 s。诊断：accept 队列溢出计数（hc2）。
2. io_uring 空闲连接内存是 epoll 的 2 倍：挂起的 RECV 占着用户缓冲区（epoll 挂起时把数组还给池）。
3. io_uring 连接数少时低于 epoll（64 连接 368k 对 417k）。

### 26.5 io_uring 空闲读不占缓冲区
依据（§26.4）：每条空闲连接 io_uring 3.4 KB、epoll 1.5 KB。单次 RECV（§24.7）把读者的池化数组 pin 住交给内核，直到数据到来；
epoll 挂起时不占数组（空闲清扫把它还给池）。

做法：沿用空闲清扫（IDLE_SWEEP_MS / SWEEP_ROUNDS）。挂起跨过一整个清扫周期的 RECV 被 ASYNC_CANCEL；其 -ECANCELED CQE 到达后
取消该 fd 的读 pin、数组还给池，改挂 POLL_ADD(POLLIN)（不带缓冲区）；POLLIN 到达后从池取数组、重新 pin，提交不带 POLL_FIRST 的 RECV，
回到常规路径。提交 SQE 的步骤都在收割之后（与短写重提交相同）。取消先于数据：RECV 已收到数据则按常规完成，迟到的取消找不到目标。
读者在任何阶段被取消、超时或流被关闭，都按原来的结果结束；排队中（无操作在途）的读由收割后的处理或 shutdown 释放。
只影响挂起超过一个清扫周期的读，活跃连接的路径不变。`NETON_IO_URING_IDLE_POLL=0` 关闭（A/B）。`NETON_IO_STATS` 新增
`idle_demotions` / `idle_wakes`。

验证：`IdleReadTest`（空闲后收数据 ×3、EOF、取消、关闭；io_uring 上计数确认走了降级路径），colima Linux arm64 全部测试
io_uring / io_uring multishot / epoll 各 81/81，macOS 80/80。效果以 153 C2 每连接内存与 C1 吞吐（不得回退）衡量。

第一版判定（挂起跨过一个清扫周期，约 50 ms）净收益为负（hc4，同一二进制 `IDLE_POLL` 0 / 1，4 轮，raw `…hc4-raw.txt`）：
64 活跃 + 50k 空闲内存 158 → 83 MiB，但吞吐 0.92；1k 全活跃吞吐 0.90；50k 全活跃吞吐 0.93、内存 160 → 300 MiB。
原因：高并发下忙连接两次请求之间本来就要等很久（50k 连接约 0.3 s），忙连接也被降级，每次读变成取消 + POLL + RECV 三个操作。
现判定改为挂起时长：空闲纪元每 IDLE_READ_MS（1 s）最多前进一次（只在清扫时读时钟，不在每次读时），提交于两个纪元之前的 RECV
才降级，即挂起 1–2 s。重新按同一矩阵测量；仍无净收益则删除本节代码。

第二版（hc6 / hc7，同一二进制 `IDLE_POLL` 0 / 1）：吞吐不再回退（1k 0.99，50k 0.99），但也不省内存：64 活跃 + 50k 空闲跑 12 s，
结束时 RSS 185–189 MB 对 185–188 MB，GC 后 Kotlin 堆 181–184 MB 对 180–183 MB。数组还给池后并不释放——建立连接时的 GC 之后
稳态零分配、不再 GC，数组留在堆里；第一版省下的内存来自建立连接期间就归还、被后续连接复用，代价是忙连接的吞吐。
**结论：两版都没有净收益，代码已删除**（`IdleReadTest` 与驱动无关，保留为回归测试）。io_uring 空闲连接的内存要靠别的办法
（例如 multishot 的共享缓冲环，§17c 已有，默认关闭），另行按数据决定。

### 26.6 建立与关闭连接
**accept 队列**（hc2，`nstat`，raw `docs/benchmarks/2026-09-27-153-hc2-accept-raw.txt`）：建立连接慢的每一轮都对应
`ListenOverflows`（每次溢出，客户端的 SYN 等 1 s 重传）；不溢出的轮次 neton 与 geario 一样快（10k：0.16–0.17 s 对 0.14 s；50k：0.53 s 对 0.55 s）。
队列长度 = min(backlog, somaxconn)：neton 默认 1024，geario 2048（`ss -ltn`），153 与 Linux ≥ 5.4 的 somaxconn 为 4096。
单变量实验（hc4，同一二进制，4 轮）：backlog 4096 后溢出消失——epoll 50k 建立 2.57 → 0.54 s，io_uring 10k 1.70 → 0.15 s、
50k 6.23 → 0.81 s（geario 0.80 s）；吞吐不变。默认 backlog 改为 4096（内核再按 somaxconn 截断）。
关闭时异常的修复经 callgrind 确认（hc5）：每条连接的 `Throwable` / 栈回溯分配消失，`closeStream` 3.88 → 1.88 次分配。

**关闭时的异常**（hc3，callgrind，2000 条连接对 0 条的分配位置差，raw `…hc3-conn-allocs-raw.txt`）：每条连接关闭时
构造两个 `ClosedException`（`Throwable` 约 14 次分配、2 次栈回溯）——`closeStream` 把它们作为参数传给 `finishRead / finishWrite`，
而这两个函数在没有挂起操作时直接返回。现在只在有挂起的读 / 写时才构造（io_uring multishot 的关闭路径同样）。
每条连接其余的对象是一连接一协程本身：`launch` 的协程与其子 Job 节点、取消监听节点、处理函数的状态机、`ReactorStream`、`Buffer`。

### 26.7 Linux 默认驱动按数据决定
现状：Linux 默认 io_uring（不可用时 epoll），依据是早期 12 连接矩阵（§24.9：4 KB / 64 KB io_uring 略好）。§26 的高并发数据里 io_uring
从未胜过 epoll：128 B 时 1k 连接 0.92、10k 与 50k 持平，64 活跃 + 50k 空闲 0.70–0.94。
实验（hc8，当前 HEAD，同核分配同 §26.2）：负载 128 B / 4 KB / 64 KB × 连接 64 / 1k / 10k（64 KB 只到 1k），外加 64 活跃 + 50k 空闲；
驱动 epoll、io_uring、io_uring multishot（参考）；4 轮，顺序轮换。默认驱动取多数格胜出且没有大幅落后格的一方；两者各有大胜时保留
按场景选择的开关并在文档写明。

结果（hc8，v51，4 轮均值，raw `docs/benchmarks/2026-09-27-153-hc8-driver-matrix-raw.txt`），io_uring / epoll 与 multishot / epoll：
128 B：64 连接 0.90 / 0.90，64 + 50k 空闲 1.12 / 0.94，1k 1.10 / 0.99，10k 0.98 / 0.91；4 KB：64 1.04 / 1.01，1k 1.01 / 0.86，10k 0.94 / 0.84；
64 KB：64 1.05 / 0.57，1k 1.00 / 0.52。Jain 全部 ≥ 0.95。
io_uring 对 epoll 4 胜 3 平 2 负（−10 %、−6 %），且 hc1 / hc6 在同样两格（1k；64 + 50k 空闲）上得到相反的结论（0.92；0.70–0.94）：
两者之差在本机轮间波动之内。**默认不变（io_uring，不可用时 epoll）；multishot 在大负载只有一半，维持默认关闭。**

### 26.8 GC 设置（msgtrans rpc，高并发）
msgtrans SPEC §14.1：自动调节（默认）下 1k 连接每秒 4.3 次 GC、停顿占墙钟 0.04 %，10k 连接几乎不 GC——GC 不是瓶颈，剩余成本在到达安全点的等待。
固定目标堆（`GcTuning`，64 / 256 MiB）在 1k / 10k 连接下 GC 次数多 20 倍、吞吐 −11 至 −14 %（8 对全负），与 §24.6 的 12 连接结论（+5.5 %）相反，
原因待查。建议改为默认（自动调节），`GcTuning` 文档已改。
原因已查明（K/N 2.4.0 源码 `gcScheduler/common/cpp/HeapGrowthController.hpp`）：触发线 `triggerHeapBytes_` 只在构造时按初始 10 MiB 算一次（0.9 × 10 MiB），
`updateBoundaries` 只在自动调节开启时重算它；自动调节关闭时只更新目标、不更新触发线。所以"关闭自动调节 + 目标 64 / 256 MiB"实际是"对象超过 9 MiB 就 GC"：
1k 连接存活 16 MiB 时每轮都立刻再触发；§24.6 的 12 连接存活很小，9 MiB 反而比自动调节的约 4.5 MiB 宽松，所以当时是 +5.5 %。
改法：`GcTuning.setMinHeap(mb)`（环境变量 `NETON_IO_GC_MIN_HEAP_MB`）保持自动调节，只抬高 `GC.minHeapBytes`——下一目标仍是 `存活 / 0.5`，但不低于下限，
触发线每次 GC 后照常更新。旧的 `fixTargetHeap` 删除（0.1.0 之后加入，未发布）。`GcTuningTest` 复现：16 MiB 存活、约 200 MiB 垃圾，
关闭自动调节 + 目标 64 MiB 为 20 次 GC，`setMinHeap(64)` 为 5 次（与 200 / 9 ≈ 22 和 200 / 41 ≈ 5 吻合）。高并发效果由 msgtrans mt15 测量。
mt15（msgtrans §14.2）：下限 64 MiB 时 1k 连接 GC 次数与安全点等待约降为 1/5，吞吐 +2 %（两种连接数各 3/4 轮胜，处在本机噪声边缘），p99 不变差，
堆 24 → 63 MiB；256 MiB 无更多收益。默认仍是自动调节（GC 设置属于整个进程，库不替应用设置）；内存充裕的服务端可设 `NETON_IO_GC_MIN_HEAP_MB=64`。

## 27. 服务端补齐（2026-09-27 用户："把这 1-5 实现了"）
对照 geario 源码（`~/projects/Neton/geario`）余下的服务端能力。原则同 §26.1：协程常规写法；性能项必须实验证明收益才进默认。

### 27.1 进程信号与平滑停机
geario：服务端内置信号处理，SIGINT 立即停，SIGTERM 平滑停，SIGQUIT 按配置；`signal()` 让应用等待信号。
- `suspend fun awaitSignal(vararg signals: Signal = [Int, Term, Quit]): Signal`：挂起直到收到其中一个。首次等待某个信号时才为它安装处理函数
  （不改变应用没要求处理的信号的默认行为）。
- 实现：信号处理函数里不能执行 Kotlin 代码。C 垫片（`sigshim.def`，POSIX）的处理函数只向自管道写一个字节（写端非阻塞，满了丢弃，同一信号
  已在管道里即可）；一个专用线程阻塞读管道，把信号交给等待者（`CompletableDeferred`，在等待者自己的调度器上恢复）。Windows：
  `SetConsoleCtrlHandler`（Ctrl-C → Int，Ctrl-Break → Quit，关闭 / 注销 / 关机 → Term），经事件对象交给同一个专用线程。
- `suspend fun TcpServerGroup.shutdownOnSignal(gracefulTimeoutMillis = 30_000)`：Int → 立即停（`shutdown(0)`），Term / Quit → 平滑停。
- `serveTcp(..., shutdownOnSignals = true, shutdownTimeoutMillis = 30_000)`：默认开启，与 geario 相同。
- 验收：进程内 `raise(SIGTERM)` 使 `awaitSignal` 返回 Term；`serveTcp` 收到 SIGTERM 后等活动连接结束再返回，收到 SIGINT 立即返回。

### 27.2 反应器线程绑核
geario：`enable_affinity()`。neton：`listenGroup(..., pinThreads = false)` / `serveTcp(..., pinThreads = false)`：反应器 i 绑到进程允许的
CPU 集合（`sched_getaffinity`）中的第 i 个（取模）。Linux / Android 用 `sched_setaffinity`（C 垫片），Windows 用 `SetThreadAffinityMask`；
Apple 不支持线程绑核（返回 false，不报错）。性能项：153 上同一二进制 `NETON_IO_AFFINITY` 0 / 1 配对测量，有收益才考虑默认开启。

### 27.3 Service 并发上限
更正 §27 前的说法："单连接在途上限"在 neton-io 层不存在——`serve()` 在一个连接上顺序处理请求。geario 的 `InFlight` 限制的是一个 Service 实例
（所有使用它的连接合计）的并发调用数，满了就不再读取，形成反压。
- `fun <Req, Res> Service<Req, Res>.limitInFlight(max: Int): InFlightService<Req, Res>`：用 kotlinx `Semaphore`；达到上限的调用挂起，其连接因此
  停止读取（TCP 反压）。`inFlight` 可读。每次调用多一个协程帧（选用才有）。
- 验收：max = 3、并发 10 个调用时同时进行的不超过 3 个且全部完成；取消等待中的调用不泄漏许可。

### 27.4 内存流（测试用）
geario：`IoTest`。neton：`memoryStreamPair(capacity = 64 KiB): Pair<IoStream, IoStream>`（commonMain），两端各是一个 `IoStream`，
线程安全（kotlinx `Mutex`）。按字节容量反压（写满挂起，读走后继续）；`shutdownOutput` → 对端读到 EOF；`close` → 对端 EOF、对端写抛异常、
本端挂起的读写抛 `ClosedException`。上层（msgtrans、PulseKit）的协议测试可不走真实 socket。
- 验收：`Framed` + `LineCodec` 经内存流回显；容量反压；半关闭；关闭；跨线程使用。

### 27.5 多个监听端口共享一组反应器；并行 DNS
- `suspend fun TcpServerGroup.listenAlso(host, port, options, maxConnections, acceptMode): TcpServerGroup`：在同一组反应器上再开一个监听端口，
  各自 `serve(handler)`、各自上限 / 暂停 / 停机。反应器按引用计数，最后一个监听组停机时才停。只能在反应器 0 上调用。
- 名字解析：`getaddrinfo` 从单个解析线程（串行）改到 kotlinx `Dispatchers.IO`（线程池，并行），恢复回调用者的反应器。
- 验收：两个端口各自回显、各自停机互不影响、全部停机后工作线程退出；并发解析多个名字全部成功。

### 27.6 缺陷：Apple 上对端在 accept 之前关闭 / 重置，服务端写入时进程死于 SIGPIPE（实现 §27.1 时发现）
`ConnectionFaultTest` 单独运行必现退出码 141（SIGPIPE）；此前全量运行能过是时序碰巧。原因（C 程序验证）：对端在服务端 `accept()` 之前
已关闭（Unix 域）或已 RST（TCP）时，macOS 对该 socket 的 `setsockopt(SO_NOSIGPIPE)` 返回 EINVAL，选项没设上，此后任何 send 都给整个进程
发 SIGPIPE——任一"连上、发数据、立即 RST"的客户端都能打死 macOS / iOS 上的 neton 服务端。TCP 正常 FIN（含半关闭）时该选项仍可设置。
修复：设置失败时不丢弃连接（Unix 域上对端写完就关闭，缓冲里仍有数据），而是标记该 fd，读照常（先读完对端发的数据，再 EOF），写直接抛
EPIPE 的 `IoException`，不调用 send。回归测试：`UnixSocketTest.peerGoneBeforeAcceptStillDeliversAndWritesFail`；`ConnectionFaultTest` 单独连跑 5 次通过。

**§27.1 / §27.2 实现记录**：C 垫片 `posixshim.def`（全部 POSIX 目标）与 `winshim.def` 的新增函数；`Signals.kt`（公共：`Signal`、`awaitSignal`、
等待者列表与读信号的专用线程）、`Signals.posix.kt`（自管道 + sigaction）、`Signals.mingw.kt`（控制台事件）。`serveTcp` 默认处理信号：
`until` 与信号两条停止路径各有自己的标志（共用一个时，`until` 停止后会去等一个永远不来的信号——`MultiReactorTest` 挂起，已修）。
测试进程一旦为 SIGTERM 装了处理函数，`kill` / `timeout` 的 SIGTERM 就不再结束它（这正是语义）；卡住时只能 SIGKILL。
绑核：在启动任何线程前记录允许的 CPU 集合（新线程继承创建者的掩码）；`AffinityTest` 在 Linux 上确认两个反应器线程各只能运行在一个 CPU 上
（Apple 跳过）。测试：macOS 86/86，colima Linux arm64 io_uring / multishot / epoll 各 87/87；mingwX64、Android、iOS 编译通过。

**§27.3 / §27.4 实现记录**：`InFlightService` / `limitInFlight`（`Service.kt`，kotlinx `Semaphore`）；`memoryStreamPair`（`core/MemoryStream.kt`，
每个方向一个有界字节队列，自旋锁只保护一次最多 `capacity` 字节的拷贝，等待用 `CompletableDeferred`）。原来测试目录里的 `memoryPair`
（无界 channel，无反压、无半关闭）改为调用它。测试：`InFlightServiceTest`（10 个并发调用峰值 3；取消等待者不漏许可）、`MemoryStreamTest`
（Framed 回显、容量反压、半关闭、关闭、跨线程 20 万字节）；macOS 93/93。

**§27.5 实现记录**：`ReactorGroup` 加引用计数（`retain` / `release`，只在反应器 0 上改），`TcpServerGroup` 的 `serve` 结束与 `shutdown` 各自只释放一次；
`listenAlso` 在同一组反应器上开新端口（反应器已停时报错，非反应器 0 调用报错）。名字解析改为 `withContext(Dispatchers.IO)`（POSIX 与 Windows），
去掉单线程的 `neton-resolver` Worker。测试：`SharedReactorsTest`（两个端口各自回显、第二个端口用到两个反应器、先停一个另一个照常、
都停后工作线程退出、之后 `listenAlso` 报错；32 个并发解析全部成功）。macOS 95/95，colima Linux arm64 io_uring / multishot / epoll 各 96/96；
mingwX64、Android、iOS 编译通过。

**§27.2 测量**（hc9，v52，同一二进制 `NETON_IO_AFFINITY` 0 / 1，服务端 `taskset 0,1` 两个反应器，4 轮顺序轮换，raw `docs/benchmarks/2026-09-27-153-hc9-affinity-raw.txt`）：
绑核 / 不绑核 epoll 128 B 64 / 1k / 10k 连接 1.10 / 0.96 / 1.00，4 KB 1k 0.94；io_uring 0.99 / 1.04 / 1.03，4 KB 1k 0.97。同一格轮间波动 20–40 %，
比值方向不一致（epoll 64 连接的 1.10 来自一轮 459k 对 353k）——**没有可测量的收益，`pinThreads` 保持默认关闭**，作为选项保留（进程独占机器、
不受 `taskset` 限制时可自行测量）。

### 27.7 生命周期修正（2026-09-27 GPT 审查，三处 P1 全部成立）
**内存流**：(1) `shutdownOutput` 之后 `write` 仍能成功，对端读到 EOF 后又读到数据；(2) 本端的关闭检查在锁外、等待者登记在锁内：
检查通过后另一线程完成 `close()`（此时无等待者可唤醒），读者随后登记并永久挂起（写同理）。修复：本端关闭 / 半关闭状态与等待者登记都在
同一把锁内判断——读在锁内看 `readerGone`，写在锁内看 `writerClosed` / `writerDone` / `readerGone`。测试用钩子（`memoryStreamPairWithHook`）
在"开放检查之后、取锁之前"精确地关闭流，不靠 `delay`；把修复临时去掉时这些测试失败（读挂起被超时抓到），修复后通过。

**信号**：处理函数一旦安装就不再恢复，后台线程在没有等待者时直接丢弃信号，而 `serveTcp` 默认开启——库在替宿主进程做进程级决定
（neton-io 还随 PulseKit SDK 运行在 iOS / Android 应用里）。修正：
- 按信号计数等待者：第一个等待者出现时安装并保存原来的处理方式，最后一个离开时恢复原样（POSIX `sigaction` 旧值；Windows 移除控制台处理函数）。
  只有有人等待期间进程的信号行为才被改变。
- `serveTcp(shutdownOnSignals = false)`：默认不接管信号，应用入口显式开启（geario 默认开启，是因为它的 server 就是入口）。`listenGroup`
  不碰信号；`TcpServerGroup.shutdownOnSignal` 是显式调用。
- 停机期间再次收到 Int / Term / Quit：立即取消剩余连接（"再按一次 Ctrl-C"）。平滑停机期间处理函数保持安装，停机结束后恢复。
- 验证：进程内检查等待结束后处理方式恢复为原值；端到端用独立子进程（测试二进制自身以子进程模式启动）：`serveTcp` 收 SIGTERM 平滑停、
  第二个信号强制停、返回后子进程对自己发 SIGTERM 必须按默认行为被终止。

**说明**：`limitInFlight` 只限制正在执行的 Service 调用数，不是连接数、排队请求数或内存的上限，不是完整的过载保护（与 `maxConnections`
一起用）。§27.2 的绑核结论只针对 2 个反应器、`taskset` 限定 2 核的配置，不能外推到 16 / 32 核。

**§27.7 验证**：macOS 103/103；colima Linux arm64 io_uring / multishot / epoll 各 105/105（含 `SignalChildProcessTest`：子进程平滑停、
返回后 SIGTERM 以默认行为结束，退出码 143）；mingwX64、Android、iOS 编译通过。反向验证：把恢复逻辑临时改为空操作，子进程测试
（子进程打印 "survived"）与全部检查恢复的进程内测试失败；把内存流锁内检查临时去掉，四个新测试失败（读挂起被超时抓到）。

### 27.8 生命周期修正（二）（2026-09-27 GPT 第二轮审查，四处全部成立）
1. **第二次停机信号可能丢失**：`holdingSignals` 只让处理函数保持安装，它的 deferred 只能完成一次。第一次信号返回后、`force` 协程登记前，
   watcher 读到的第二次信号只投给已完成的 deferred——事件丢失。修正：停机全程用**一个持续订阅**（`Channel`，信号到达即入队），第一次与
   第二次都从同一个订阅里取，中间不存在无人接收的时刻。测试：进程内连发两次 SIGTERM（不留间隔），第二次必须强制停机。
2. **暂停等待期间被取消，已 accept 的 fd 和连接配额泄漏**：`acceptFd()` 成功后的 `pauseGate.await()` 不在清理保护内。修正：从 accept 成功到
   交给连接协程之间用 `try / finally`，只有交接成功才转移清理责任（关闭 fd、`releaseSlot`）。测试：暂停状态下有连接到来，取消 serve，
   `activeConnections` 归零，客户端读到 EOF。
3. **子反应器启动失败不传回**：worker 在发布 `ready` 之前抛异常，只完成 `done`，主线程在 `ready.isCompleted` 上永远空转。修正：worker 以异常
   完成 `ready`；启动方看到失败就停掉已启动的 worker、关闭已绑定的监听器并抛出。测试：用测试钩子让一个 worker 启动失败，`listenGroup`
   必须抛出，端口可以重新绑定，已启动的线程退出。
4. **内存流同方向并发**：每个方向只有一个等待者槽位，并发的两个读（或写）后者覆盖前者，被覆盖者无人唤醒。修正：与 socket 流的约定一致——
   每一端同一时刻只允许一个读和一个写，重入直接抛 `IllegalStateException`（公开写入契约），不做排队。测试：并发的第二个读被拒绝；关闭仍能
   唤醒第一个。

**§27.8 验证**：每项先写能复现的测试，在旧代码上确认失败，再修复：(1) 连发两次 SIGTERM 旧代码 5/5 失败（第二次丢失，等到 3 s 超时），
修复后 5/5 通过；(2) 暂停期间取消旧代码失败（配额未归零），修复后 `ServerControlTest` 连跑 3 次通过；(3) worker 启动失败旧代码永久挂起
（被 20 s 强制超时结束，退出码 137），修复后通过且端口可立即重新绑定；(4) 去掉重入检查时第二个并发读覆盖等待槽位、测试挂起（137），
修复后通过。顺带：`launchConnection` 以 LAZY 启动，协程体开始前被取消时其 `finally` 不会执行——完成回调在"协程体未开始"时关闭 fd、
释放配额并移出连接集合（同一类交接问题；无单独测试）。全部测试：macOS 108/108，colima Linux arm64 io_uring / multishot / epoll 各 110/110；
mingwX64、Android、iOS 编译通过。

### 27.9 连接协程的回收（2026-09-27 GPT 第三轮审查，两处成立）
§27.8 顺带加的完成回调有两个问题：(1) `invokeOnCompletion` 的回调在完成该 Job 的线程上执行，不保证在所属反应器上——未启动的 Job 被别的线程
取消时，回调会与反应器并发地改反应器私有的 `connJobs`；(2) 先注册回调、后登记 Job：父作用域已取消时 Job 创建即完成，回调立即执行 `remove`
（集合里还没有它），随后 `add` 把已完成的 Job 留在集合里。
修正（换结构，不在回调上补）：连接协程以 `CoroutineStart.ATOMIC` 启动——协程体保证开始执行，哪怕开始前已被取消；登记、移出集合、关闭流、
释放配额全部在协程体里、在所属反应器上完成，`try / finally` 保证恰好一次（关闭流依赖 `close` 幂等）；开始前已取消则跳过处理函数。
去掉 `bodyStarted` 与完成回调（每连接少一次分配）。handler 结束后库也关闭流（此前取消时依赖 handler 自己关）。
测试：(a) 父作用域预先取消：handler 不执行、fd 关闭、配额释放恰好一次、集合为空；(b) 创建后、执行前从另一个线程取消（所属反应器被一个
任务占住，保证取消先于开始）：同上，且集合操作都在所属反应器上；(c) 取消与启动竞争 1000 次：回收次数等于连接数、集合为空、停机完成。

**§27.9 实现与发现**：`startConnection`（ReactorGroup.kt）以 ATOMIC 启动；`launchConnection` 只是转调。竞争测试 (c) 首次运行 3/3 失败
（300 个连接回收 286 个）——不是回收逻辑，而是**各驱动取消监听里原有的丢失唤醒**：`watchCancellation` 先查 `job.isActive`、再以
`invokeImmediately = false` 注册回调；取消恰好落在两者之间时回调不会被调用，随后挂起的读 / 写永远无人唤醒。改为 `invokeImmediately = true`
不行：`postToReactor` 在反应器线程上同步执行，唤醒会早于续体登记。修正：注册后再查一次 `isActive`，已取消则注销并抛出（操作尚未挂起）；
注册后的取消只能来自别的线程，其唤醒排在这次挂起之后。ReadinessReactor、UringReactor、IocpReactor 与 msgtrans `ReactorQueue` 同样修正。
之后 (c) 3/3 通过（约 290 个 handler 开始执行、约 10 个在开始前被取消，两条路径都走到）。(a)(b) 针对的旧回调结构没有在旧代码上单独复现
（它们调用新提取的 `startConnection`）。全部测试：macOS 111/111，colima Linux arm64 三种配置各 113/113；msgtrans macOS 43/43、Linux
io_uring / epoll 各 43/43；mingwX64、Android、iOS 编译通过。

### 27.10 强制停机漏掉排队中的连接（2026-09-27 GPT 第四轮审查，成立）
§27.9 把 `jobs.add` 移进协程体后：连接协程排队未执行时集合里没有它；`cancelConnections` 只取消集合快照，之后该协程启动、未被取消、
进入 handler，`shutdown` 一直等配额归零。同一类窗口更早还有一段：反应器 0 accept 后经 `runOn` 交给反应器 i 的任务尚在队列里。
修正：每个监听组一个持续的强制停机状态——`cancelConnections` 先置位、再取消快照；连接协程登记后、进入 handler 前检查，已置位则不进
handler、直接清理。在所属反应器上"协程体"与"取消快照任务"串行：协程体先跑则已在集合里、被快照取消；快照先跑则状态已在投递快照之前置位。
平滑停机不置位（不误杀允许完成的连接）；状态只属于该监听组（`listenAlso` 的其他监听组不受影响）。
测试（生产路径）：反应器 1 被一个任务占住，第二个连接交给它后排在队列里；真正调用 `shutdown(0)`，再放开反应器 1：交给反应器 1 的连接的
handler 不执行、客户端读到 EOF、配额归零、`shutdown` 返回。
**§27.10 验证**：该测试在修复前 3/3 挂起（`shutdown(0)` 永不返回，40 s 强制超时，退出码 137），修复后连跑 5 次通过。全部测试：macOS 112/112，
colima Linux arm64 io_uring / multishot / epoll 各 114/114；mingwX64、Android、iOS 编译通过。
