# neton-io — 规格说明（SPEC）

> **协程原生的高性能异步 I/O 框架（Kotlin/Native 优先），Neton / Pulse 全栈的统一 I/O 底座。**
>
> 架构对标并移植 [`geario`](../../Neton/geario)（ntex 网络层的单 crate 提取，性能 ≈ ntex），
> 采用 ntex-io 的 **buffered filter** 模型，映射到 Kotlin 协程。
> 性能北极星：Golang 最强协程网络库（**gnet / 字节 netpoll**）一档；Rust `ntex/geario` 为参考天花板。
>
> 状态：草案 v0（API 未稳定）· 日期：2026-09-12 · 作者：zoujiaqing


## 导读与章节状态（2026-09-27，供整体评审）

本文件按时间追加，早期章节的部分结论已被后续章节取代。**权威的定位、边界与路线图见 §28**；历史章节保留原文，不改写。评审时以下表为准：

| 章节 | 状态 | 说明 / 取代者 |
|---|---|---|
| §1 定位与目标 | 被取代 | "neton-io-http / 替代 Ktor"改为：HTTP 等协议是 neton-io 之上的独立库（§28.1、§28.7） |
| §2 非目标 | 部分被取代 | "TLS 走原生"改为 TLS 不在 neton-io（§28.1）；其余有效 |
| §3 设计原则 | 大体有效 | 3.4 "off-heap 缓冲"实际为池化 `ByteArray` + pin（§23.7、§24）；3.5 显式驱动失败即报错有效 |
| §4 分层 | 部分被取代 | 单一产物有效；tls / http 层移出；testkit 改为独立产物（§28.6） |
| §5 核心模型 | 部分被取代 | 挂起改为原始续体（§19.3）；`Service` 的 readiness 未实现，现为 `limitInFlight`（§27.3） |
| §6 驱动矩阵 | 部分被取代 | `nw` 未做且不在计划；`testing` 即 `memoryStreamPair`；现状以 §15、§20 为准 |
| §7 TLS 即 Filter | 被取代 | TLS 不在 neton-io；`Filter` 作为包装扩展点保留（§28.1） |
| §8 HTTP 能力 | 被取代 | §28.7（独立的 HTTP/1.1 模块，验证公开接口） |
| §9 性能目标 | 部分被取代 | 验收指标以 §26.1、§28.4 为准；gnet / netpoll / Ktor 基线未做 |
| §10 依赖 | 被取代 | §28.1 |
| §11 里程碑、§11.1、§11.2 | 历史 | 平台范围以 §20 为准，Linux 优先以 §24 为准 |
| §12 待定问题 | 已结 | 1 JVM：不做；2 内存驱动：`memoryStreamPair`；3 TLS：移出范围；4 io_uring 回退：§15；5 缓冲所有权：§23.7 |
| 附录 A、B | 历史 | — |
| §15 实现现状 | 有效（部分数据旧） | io_uring 读默认改为单次 RECV（§24.7） |
| §16 多反应器 | 已完成 | — |
| §17 / 17b / 17c | 历史 | 17b 已否决 |
| §18 | 已完成 | 18.3 由 §26 覆盖；18.5 密码学：随 TLS 移出范围 |
| §19 | 已完成 | 19.5 公平性结论有效，完整验证见 §28.4 |
| §20 全平台 | 有效 | Windows 未实测（§28.5） |
| §21 TLS 1.3 | 撤回 | 用户后来决定 TLS 不在 neton-io 范围 |
| §22 IOCP 设计 | 已实现 | §23.1；未实测（§28.5） |
| §23 | 已完成 | 23.1 未实测；23.5 默认 backlog 已改为 4096（§26.6） |
| §24 | 已完成 | 24.6 `fixTargetHeap` 被 `setMinHeap` 取代（§26.8）；24.8 默认关闭 |
| §25 macOS | 基线 | 高并发未测 |
| §26 | 已完成 | 26.5 已删除（无净收益）；26.7 默认驱动不变；26.8 GC 结论有效 |
| §27 | 已完成 | 27.2 默认关；27.6–27.10 为缺陷与生命周期修正记录 |
| §28 | **待评审（修订 3）** | 当前有效契约索引（§28.0）、定位、API 分级、反应器生命周期与 `ReactorResumer`、公平性与负载验证、Windows、`IoStream` 契约与一致性套件、HTTP/1.1 子集、可观测性、数据报需求、执行顺序、读取前准入、产物坐标与包名（§28.13） |

`TODO.md` 是 §18–§23 时期的执行清单，已不再维护；当前执行顺序见 §28.11。

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
| `io-testkit` | **内存 driver** + conformance 测试（Phase 0 零 cinterop） | `io/testing.rs` |

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


**GC 线程优先级（2026-09-28，http SPEC §11 的测量）**：K/N 的 GC 协调者以 `sched_yield` 自旋等待各线程到达安全点。GC 线程与反应器共用一个核时，
调度器让自旋的线程占满整个时间片（≈6 ms），反应器（它所等待的线程）无法运行：每次回收的"到达安全点"为 5,990 µs，而暂停本身只有 12–15 µs。
新增 `GcTuning.lowerGcThreadPriority(nice)`（`NETON_IO_GC_THREAD_NICE`，Linux / Android，其他平台返回 0；与 `setMinHeap` 一样只由应用调用）：
降低 GC 线程的调度优先级后到达安全点为 1 µs，HTTP hello world 单核 p99 7.0 → 0.94 ms；60 s 满载单核下堆稳定，回收照常进行。
运行时的 GC 线程在单核上可能尚未启动，该调用最多等待 500 ms。

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

### 27.11 `runReactor` 的块失败时挂起或使进程中止（2026-09-28，实施 §28.12 的反向验证时发现）
`Reactor.run` 在根协程里 `try { block() } catch { failure = t }`，循环结束后再抛出 `failure`。两个后果：
- 块自己失败（包括逃出的 `withTimeout`）时异常被吞掉，根作业按"正常完成"等待子协程；停在 accept 循环或读上的子协程无人取消，`runReactor` 永不返回。
- 子协程失败使根作业失败，而根作业是无处理器的顶层 `launch`：异常交给未捕获异常处理器，**整个进程中止**（测试二进制里其余测试随之不再运行）。
这是 §28 之前就存在的缺陷（在 §28.11 第 1 步之前的提交上复现）。表现：断言失败的测试不报失败而是挂起或中止。
修正：根协程改为 `async`，语义与 `runBlocking` 一致——块或任一子协程失败即取消其余部分，循环结束后抛出**原始**异常（子协程的断言失败不再
被 `CancellationException` 顶替）。测试 `ReactorLifecycleTest` (f)：块失败时停在 accept 与读上的两个子协程都被取消、抛出原异常；子协程失败时抛出
该子协程的异常；逃出的 `withTimeout` 同样返回。修正前该测试挂起（20 s 强制超时）。
**§27.11 验证**：单独在上一提交之上：macOS 118/118，colima Linux arm64 io_uring（multishot）/ io_uring 单次 RECV / epoll 各 120/120。

### 27.12 跨线程唤醒与唤醒管道关闭的竞争（2026-09-28，Linux 上偶发 SIGPIPE）
`wakeup()` 先检查 `wakeClosed` 再写唤醒管道，两步不是原子的：投递线程检查通过后、写入之前，反应器可能已退出并关闭管道，写入落到已关闭或
已被复用的 fd 上（复用为套接字时 `write` 触发 SIGPIPE，测试进程被杀）。`ReactorLifecycleTest` (c)（300 个反应器生命周期内的投递与关闭竞争）在
colima Linux epoll 上 30 次中失败 7 次（退出码 141）。IOCP 同理：`PostQueuedCompletionStatus` 可能投到已关闭或复用的句柄，且原先在关闭端口之后
才置位。修正：唤醒者先登记（`wakers` 计数），再检查 `wakeClosed`；关闭时先置位、等到没有在途的唤醒者，再关闭管道 / 端口（两者都是顺序一致的
原子操作，必有一方看到另一方）。IOCP 改为在关闭端口之前调用。修正后同一测试 epoll / io_uring 各 30 次 0 失败。

## 28. 底座定位、契约与路线图（2026-09-27，修订 3，待整体评审；评审通过前不写代码）

用户："neton-io 就仅仅是 io 和网络层的底座，类似于 geario 和 tokio，形成一些标准化的底座建设，别人可以基于 neton-io 实现 http 1.1
websocket http/2 quic http/3 的库，最终可能这些库又可以被 neton 框架使用。" 并："先把所有规划都一步到位的落实到 spec，然后我们再一次性的评审
整个 spec 体系。评审没问题就一步到位的落实技术开发和验证。"
修订 2 并入 GPT 对修订 1 的评审（七处全部成立）：停止后的恢复须有统一的接受 / 拒绝结果；流的取消与缓冲所有权按退出方式逐一规定；HTTP/1.1 子集
冻结消息边界与拒绝策略；公平性按负载条件分三类；能力按项声明；补读取前准入；可观测性定义安全读取与指标来源；增加当前有效契约索引。
修订 3 并入 GPT 对修订 2 的评审（四处阻断全部成立，均为修订 2 新方案与既有契约冲突）：停止分为"停止准入 → 完成或取消存量任务 → 回收内核资源
→ 关闭投递入口 → 退出"，存量任务的恢复在关闭入口前一律接受；撤回拒绝后交给 `Dispatchers.IO` 展开；撤回"关闭时摘下数组"，改为挂起操作在内核
不再访问缓冲后才以终态恢复；准入区分"等待输入"与"请求处理"，空闲连接不持有许可；公平目标先定义再选指标；过载延迟绑定排队期限；并确定
产物坐标与包名（§28.13）。
实施方式：规划一次完整，**实施连续进行、每一步单独验证并提交**，不把所有改动留到最后一起验收。

### 28.0 当前有效契约索引（规范性内容只看这里列出的条目）
| 领域 | 规范条目 |
|---|---|
| 字节流 `IoStream` | §28.6（本次修订取代 IoStream.kt 现有 KDoc 中不一致之处；实现时 KDoc 与之对齐） |
| 反应器生命周期与 `ReactorResumer` | §28.3 |
| 服务端组（`listenGroup` / `listenAlso` / `serveTcp` / `TcpServerGroup`） | §23.4、§27.5、§27.8、§27.9、§27.10；默认 backlog 4096（§26.6） |
| 信号 | §27.1、§27.7、§27.8 |
| 内存流 `memoryStreamPair` | §27.4、§27.7、§27.8；能力声明按 §28.6 |
| Service / 并发上限 / 读取前准入 | §27.3、§28.12 |
| 公平性与性能验收 | §19.5（每连接每轮一次读）、§28.4（验证方法与门槛）、§28.4 验收指标 |
| GC 建议 | §26.8（默认自动调节；`setMinHeap` 可选） |
| 可观测性 | §28.8 |
| 平台支持 | §20；Windows 以 §28.5 验收为准 |
| 产物坐标、包名与框架集成边界 | §28.13 |
| 协议库建设方法 | §28.14 |
| 协议库提出的底座缺口 | §28.15 |
其他章节是历史记录或已完成工作的说明（见开头导读），不构成契约。

**兼容性**：`com.netonstream:neton-io:0.1.0` 已发布（2026-09-25），保持不动。§28.3、§28.6、§28.12 的破坏性变更（`ReactorResumer.resume`
返回值、`IoStream` 可选操作的默认行为、`capabilities`、关闭语义）与产物更名（§28.13，新坐标 `com.netonstream:io`）一起在 0.2.0 发布，发布说明列出
迁移方式；不在 0.1.x 上做破坏性变更。

### 28.1 定位与边界
```
Neton 框架 / PulseKit / 其他应用
                ↓
HTTP/1.1 · WebSocket · HTTP/2 · QUIC/HTTP/3 · msgtrans（各自独立的库）
                ↓
neton-io：异步 I/O、网络、调度、缓冲、背压、生命周期
                ↓
epoll / kqueue / io_uring / IOCP
```
- **neton-io 负责**：I/O 契约；执行模型（连接归属、跨线程投递、多反应器、公平性、阻塞工作移出反应器——用 kotlinx `Dispatchers.IO`，
  如 §27.5 的名字解析）；资源控制（缓冲复用、连接上限、读取前准入、有界排队、背压、可靠停机）；扩展接口（包装字节流、编解码器、组合服务，
  不需要访问 fd 表或调度队列）；可观测性。
- **不在 neton-io**：HTTP 各版本、WebSocket、TLS（独立模块，经 `Filter` 包装 `IoStream`）、QUIC 的重传 / 拥塞控制 / 流管理、HTTP/3 语义、压缩。
- **目标**：在协议正确、取消安全、资源有界、调度公平的前提下，持续对标领先实现提升性能（§28.13）。
- **成熟度判据**：msgtrans 与独立的第二协议实现（§28.7）都只依赖公开接口工作；实现中需要改 neton-io 的地方逐一作为底座缺口记录并修正。
  现状：msgtrans 只用 13 个公开符号（`IoStream`、`Framed`、`Io`、`Encoder` / `Decoder`、`Buffer`、`connect` / `listen` / `listenGroup`、
  `TcpServerGroup` / `TcpListener`、`IoException` / `ClosedException`、`ReactorResumer`）。

### 28.2 公开 API 分级
| 级别 | 内容 | 承诺 |
|---|---|---|
| 稳定契约 | `IoStream` / `Filter` / `StreamCapability`、`Buffer` / `Bytes` / `BufferPool`、`Codec` / `Framed` / `Io`、`Service` / `serve` / `limitInFlight` / `Admission`、`connect` / `listen` / `listenGroup` / `listenAlso` / `serveTcp` / `TcpServerGroup`、`SocketOptions`、Unix 域套接字、`awaitSignal` / `shutdownOnSignal`、`memoryStreamPair`、`ServerStats` | 语义由 §28.0 所列条目约束；破坏性变更只在次版本号升级时、先改 SPEC |
| 可选性能扩展 | `ReactorResumer` / `reactorResumer` | §28.3；普通协议实现不需要它 |
| 运维 / 诊断 | `GcTuning`、`GcStats`、`NETON_IO_*` 环境变量、`NETON_IO_STATS` | 可用，格式不承诺稳定；正式指标见 §28.8 |
| 基准程序 | `echoServer`、`echoClient` 等 | 不属于库 API |

### 28.3 反应器生命周期与 `ReactorResumer`
**现状**：所属线程上 `resume` 把续体放入 Any 恢复环（排队，不在调用处执行）；其他线程上退回 `intercepted().resume`（外部队列 + 唤醒）。
不观察取消；停止后的行为未定义。修订 1 的"所属线程抛异常、其他线程静默丢弃"与修订 2 的"STOPPING 立即拒绝外部投递、拒绝后交给
`Dispatchers.IO` 展开"均**撤回**：前者失败语义因线程而异；后者在请求停止时就拒绝了存量子协程终结所需的恢复（DNS 结果、跨线程结果、取消通知），
并把本应在反应器上运行的清理（反应器私有的集合、流、缓冲池）转到别的线程。

**生命周期**（原子状态，只前进）：
1. `RUNNING`：一切照常。
2. `DRAINING`（请求停止）：**停止新业务准入**——本反应器上的监听器关闭、不再接受新连接，由组的停机策略决定存量连接是等待完成还是取消
   （§23.4 / §27.10）。**一切投递与恢复照常接受**：存量协程的终结路径（取消通知、跨线程结果、名字解析结果、清理任务）都依赖它们。
3. `CLOSING`：只在同时满足以下条件时进入——根作业的全部子协程已结束；完成式驱动（io_uring / IOCP）没有在途内核操作（全部 CQE / 完成包已收割）；
   本地任务队列与各恢复环为空。进入时把外部队列头原子地换成"已关闭"哨兵（与外部投递同一个 CAS 位置），并执行一次最后的排空。
4. `STOPPED`：循环退出，释放 ring、唤醒管道等驱动资源。
- **不变式**：进入 `CLOSING` 时不存在属于本反应器根作业的挂起续体（全部子协程已结束）。因此只有在 `CLOSING` / `STOPPED` 才可能出现的拒绝，
  不会让任何合法的子协程无法结束；被拒绝的投递只可能来自不属于根作业的协程（使用本反应器调度器却不在其作用域内——不受支持的用法，KDoc 写明
  "由反应器调度的协程必须在其作用域内"）。
- "检查后、入队前停止"的竞态由 CAS 哨兵消除：外部投递要么落在哨兵之前（被接受、在最后排空中执行），要么看到哨兵（被拒绝）；所属线程上的投递与
  循环同线程，不存在交错。

**接受 / 拒绝**：
- `ReactorResumer.resume(cont, value): Boolean`、`resumeWithException(cont, error): Boolean`。`true` = 已接受：续体恰好被恢复一次，值的所有权移交。
  `false` = 已拒绝（仅在 `CLOSING` / `STOPPED`）：续体未被恢复、值未被取走，**清理责任仍在调用方**；按上面的不变式，这表示调用方的生命周期用法
  错误，调用方应释放值携带的资源并报告错误（不是正常的停机路径）。所属线程与其他线程语义相同。
- `Reactor.dispatch`（kotlinx 协程的调度入口）在 `CLOSING` / `STOPPED` 被拒绝时**抛 `ReactorStoppedException`**（`IllegalStateException` 子类）
  给发起恢复的一方，任务不执行、不转交其他线程；拒绝次数计入 §28.8。**不在其他线程运行反应器的清理代码。**
- **续体要求**：`suspendCoroutineUninterceptedOrReturn` 得到的原始续体，且该协程由这个反应器调度（调试构建断言）。
- **恰好一次（调用方的槽位约定）**：挂起方把续体放进槽位；正常恢复与取消两条路径都先在所属反应器上把续体从槽位取出，取到的一方才调用 `resume`；
  取消回调可能在任意线程运行，须先 `postToReactor` / `dispatch` 回到反应器再动槽位；取消监听按 §27.9 "注册后再查一次"。
- **用途与收益**：同一反应器上协程之间交接时省去一次调度对象。可选——不用时用普通 `resume`，结果相同。"每请求省 300–400 条指令"是 §24.11 在
  msgtrans rpc 上的实验记录，不是收益承诺。
- **测试**：(a) `DRAINING` 期间子协程等待的跨线程结果、名字解析结果、取消通知都能送达，子协程正常结束，反应器随后退出；(b) 有在途 io_uring 操作时
  不进入 `CLOSING`，操作的 CQE 收割后才退出；(c) 外部投递与"进入 `CLOSING`"竞争 10 万次（投递方为不属于根作业的测试协程），每次要么恰好执行一次、
  要么返回 `false` / 抛 `ReactorStoppedException`，无丢失；(d) `resume` 之后的代码先于被恢复的协程运行；(e) 槽位约定下取消与恢复竞争 1 万次只恢复一次。

### 28.4 公平性与负载验证
**已有记录**：§19.5 每连接每轮只读一次，Jain 由 0.29–0.40 提升到 ≥ 0.989；§26.4 高并发 C1 / C2 Jain ≥ 0.97；`FairnessTest`（任务预算防止
自我调度的协程饿死 I/O 与计时器）。
**待验证的结构性风险**：恢复队列严格优先（Unit 环 → Int 环 → Any 环 → 普通任务），每轮预算 256 个（按个数，不按时间）——恢复环持续有东西时
普通任务（跨线程投递、新协程启动、`withContext` 回到反应器）可能每轮都排不上；单个任务执行多久不受限；`serveLoop` 一次处理完已缓冲的全部帧。

**测量规程**（所有场景通用）：153；服务端 `taskset 0,1` 两个反应器，负载发生器 `taskset 2,3`；每格 5 轮顺序轮换；每轮预热 3 s（丢弃）后测量
30 s；每类连接的延迟样本 ≥ 10 万个才报告 p99、≥ 100 万个才报告 p999，否则只报告到样本足够的分位；负载发生器在所占核上的 CPU 利用率须
< 80%（每轮记录，超过则该轮作废），并记录发生器自身的回环基线延迟；同机同负载对照 geario。
**负载发生器**：`echo-client-mass` 增加开环模式（每连接按固定速率发请求，记录"计划发送时刻 → 收到响应"的延迟，避免闭环发生器的协调遗漏），
以及两类连接分别统计。饱和吞吐 `C` 先用闭环模式测出（同一服务端配置、同一载荷）。

**L1 未饱和延迟**（开环，总负载 ≤ 0.5 C）：冷连接 1000 条、每条 20 req/s；热连接 64 条、合计负载使总负载为 0.5 C，热连接每次写入 16 个请求（流水线）。
门槛：冷连接 p99 ≤ 冷连接单独运行（同样 1000 条、同样速率）时 p99 的 2 倍；最大延迟 ≤ 200 ms；无错误。
**公平目标**（先定义，再选指标）：**按连接轮转、每轮次有界**——每个就绪连接每轮得到一次服务机会（§19.5 每连接每轮一次读），一次机会内处理的
请求数有上限（当前为该次读到的全部请求；若 F3 表明需要，上限改为 N 帧 / M 字节）。在此目标下，流水线深度 16 的连接每次机会可完成最多 16 个请求，
吞吐高于深度 1 的连接是预期结果，不是不公平；不公平指的是：同类连接之间份额悬殊、某条连接长时间得不到机会、热连接的一次机会拖长所有人的等待。
**L2 饱和公平性**（闭环）：1000 条冷连接（深度 1）+ 64 条热连接（深度 16）。指标：每类内部完成数的 Jain ≥ 0.95；每类中完成数最少的连接 ≥ 该类中位数
的 0.5 倍；每条连接相邻两次完成的最长间隔 ≤ 1 s（取代"至少完成一次"）；冷连接 p99 ≤ 把 64 条热连接换成 64 条冷连接（连接数相同、轮转机会相同）
时冷连接 p99 的 3 倍（隔离出热连接批量服务的代价）。
**L3 过载有界性**（开环，总负载 2 C，配置 `maxConnections` > 0 与 §28.12 读取前准入，准入等待期限 250 ms）：服务时间固定（回显，微秒级）、排队
期限由准入等待期限约束，因此被接纳请求的延迟上限才有意义：被接纳请求的 p99 ≤ 500 ms；超出等待期限的请求按 §28.12 被拒绝并计数；服务端 RSS 增长
≤ 预先声明的上限（缓冲池上限 + 每连接固定开销 × `maxConnections` + 输出上限 × `maxConnections`，实施时按实测参数写入）；不崩溃；过载结束后 5 s 内
吞吐回到 C 的 90% 以上。
**L3 修订（2026-09-28，实施中发现原设计的两处问题；待评审）**：
1. 回显服务不挂起，每个反应器同一时刻只处理一个请求，许可（64）永远用不完，准入从不起作用（首次 L3 实测：等待 0 次）；且闭环测出的 `C` 低于
   该服务端在流水线开环负载下的实际容量（按行 epoll 闭环 491k，开环 "2 C" = 982k 时实际服务了约 981k），并未过载。改为：服务每请求挂起固定
   时间（`NETON_IO_ECHO_DELAY_MS=1`，代表一次后端调用），`C` 用同一配置的闭环测出，过载负载为 2 C。
2. "被接纳请求的 p99 ≤ 500 ms"（从客户端计划时刻起算）在过载时不可达：服务端停止读取后，已写出的请求排在内核套接字缓冲里（以字节为界、不以
   时间为界），准入与客户端的发送期限都约束不了它们；流水线连接的客户端延迟可达数秒。服务端能保证、可验证的是：
   - **许可等待有界**：≤ `acquireTimeoutMillis`，并且 ≤ 约 `maxConnections` / 许可数 × 服务时间（每条连接至多等一个许可）；由 `Admission` 的等待
     统计（只在争用路径上记录）给出分位与最大值。
   - RSS 增长 ≤ 预先声明的上限；不崩溃；超时被拒绝的连接计数；过载结束后 5 s 内吞吐回到 C 的 90% 以上。
   - 客户端观测到的延迟照常报告，不作为通过条件。客户端带重连（服务端关闭后 10 ms 重连）与 250 ms 发送期限（计划后 250 ms 仍未写出的请求丢弃、
     计为拒绝），模拟有超时的真实客户端。
**F2 恢复队列持续繁忙**（进程内）：若干协程经恢复环不停互相交接、占满每轮预算，同时测量：计时器（`delay(10)`）迟到量、其他线程 `dispatch` 进来的
任务的等待时间、新连接从 accept 到进入 handler 的时间，各 ≥ 1 万个样本。门槛：计时器迟到 p99 ≤ 20 ms（计时轮精度 10 ms + 一轮）、最大 ≤ 100 ms；
跨线程任务等待 p99 ≤ 5 ms、最大 ≤ 50 ms；新连接进入 handler p99 ≤ 20 ms。
**F3 大批量缓冲帧**（L1 负载条件下）：1 条连接一次写入 1 万个小帧（`serveLoop` 路径）+ 100 条冷连接（开环）。门槛：冷连接 p99 ≤ 无该连接时的 2 倍。
**不达标才改**，候选（每次一个变量，按验收指标测量后决定）：普通任务与恢复环轮转而非严格优先；`serveLoop` 每次最多处理 N 帧或 M 字节后让出；
按时间的轮次预算。
**验收指标**（本节及之后所有性能项）：吞吐、每请求 CPU 时间、每请求分配数（callgrind 实测，见 §28.8）、p99（样本足够时 p999）、每连接服务份额
（Jain、最小份额、最长完成间隔）；不能用饿死部分连接换来的总吞吐宣布胜出。
**规模说明**：两个反应器、4 vCPU 的结果只证明该规模。多核扩展性（≥ 16 核）作为未决项，需要相应主机，结论不外推。

### 28.5 Windows 实际运行
IOCP 与 WSAPoll 只有 mingwX64 编译链接验证。`ci/windows-validation` 已随 main 推送，GitHub Actions 上有 "Windows IOCP" 与 "Windows WSAPoll"
两个任务，本机读不到结果（API 404）。需要用户查看 Actions 页面，或给本机 `gh` 可读 Actions 的令牌。
验收：两个任务全部测试通过（nativeTest + commonTest + §28.6 一致性套件）；信号（Ctrl-C / Ctrl-Break 事件）与 `SetThreadAffinityMask` 另加 Windows
专用用例。失败项逐条记入 SPEC 修复。

### 28.6 `IoStream` 契约与一致性套件
**并发与线程**（所有流）：同一时刻至多一个读与一个写在进行（全双工允许）；同方向的第二个操作抛 `IllegalStateException`，不排队、不覆盖。
线程归属默认是"所属反应器"：只能在创建它的反应器线程上调用，其他线程调用抛 `IllegalStateException`；声明 `AnyThread` 的流可在任意线程调用，
**并发规则不变**（`AnyThread` 不等于允许同方向并发）。

**每种退出方式下的结果与缓冲**（`dst` 为读的目标缓冲，`src` 为写的来源缓冲）：
| 退出 | 读 | 写 | 缓冲可复用 |
|---|---|---|---|
| 正常返回 | 返回 n > 0，n 字节追加到 `dst`；或返回 -1（EOF），`dst` 不变 | 返回 `src` 原有的全部可读字节数，`src` 读空 | 立即 |
| `IoException` | `dst` 不变 | `src` 已按内核实际接受的字节推进 | 立即 |
| 取消（`CancellationException`）/ 超时（`TimeoutException`） | 取消生效前已到达的字节**已追加到** `dst`（不丢、不回滚），以 `dst` 为准 | `src` 已按实际发出的字节推进（不回滚） | 立即 |
| 流被关闭（`ClosedException`） | 关闭前已到达的字节已追加到 `dst` | `src` 已按实际发出的字节推进 | 立即（见下） |
- **"立即"的含义与关闭**：操作以任何方式退出时，驱动已不再引用该缓冲的数组，调用方可立即复用。完成式驱动（io_uring、IOCP）中内核可能仍在使用数组：
  取消 / 超时 / **关闭**都在内核给出完成或取消结果之后才让挂起的操作退出，`src` / `dst` 按该结果如实推进（`close()` 本身只标记关闭并发起内核取消，
  不阻塞；挂起的操作稍后以 `ClosedException` 退出）。缓冲安全优先于"立即唤醒"。（修订 2 的"关闭时摘下数组、立即唤醒"撤回：它不能在内核结果到达前
  知道 RECV / SEND 实际传输了多少，也解除不了数组的其他公开别名。）
  **现状缺陷**：io_uring 与 IOCP 关闭时立即以 `ClosedException` 唤醒挂起者，而内核操作仍在进行，调用方随即复用 `Buffer` 时内核可能仍写入原数组、
  且进度未反映；按本条修正（关闭时的唤醒改为在收割该操作的结果时进行）。§28.3 的 `CLOSING` 条件（无在途内核操作）与此一致。
- **唯一终态**：每次操作恰好一个结果：成功（计数或 -1）、`IoException`、`ClosedException`、`CancellationException`、`TimeoutException` 之一。
  取消 / 关闭与成功竞争时，任一结果都允许；无论哪个，`src` / `dst` 都如实反映已传输的字节。
- **取消后是否可继续使用**：声明 `ResumableAfterCancel` 的流在取消 / 超时后保持可用（后续读写正常，不丢已到数据）。未声明的流（如无法在记录中途
  安全停下的 TLS 包装）在取消 / 超时后**自行关闭**，之后的操作抛 `ClosedException`；不得假装可继续。
- **对端结束的三种情况**分开规定并分开测试：对端正常关闭（FIN）→ 读完已到数据后返回 -1；对端重置（RST）→ 读 / 写抛 `IoException`
  （`errno` 为平台的连接重置码），RST 之前内核已丢弃的数据不保证送达；包装层协议错误（如 TLS 记录损坏）→ `IoException` 的子类，流随即关闭。
- 其余必选契约：`read` 从不返回 0（无数据时挂起）；`write` 写完全部或抛出；`close` 幂等，挂起中的操作收到 `ClosedException`，关闭后调用抛
  `ClosedException`；`writev` 按顺序、等价于逐个 `write`；写入任意拆分，读端拼接一致（逐字节拆分的模糊测试）。

**能力按项声明**（`val capabilities: Set<StreamCapability>`，默认空）：`HalfClose`（`shutdownOutput`）、`ReadTimeout`、`WriteTimeout`、`IdleTimeout`、
`AnyThread`、`ResumableAfterCancel`。未声明时调用对应操作抛 `UnsupportedOperationException`（超时参数为 0 = 关闭，始终允许）；不静默无效。
`Framed` 只在配置了帧读取速率时要求 `ReadTimeout`，不满足时在构造时报错。**包装规则**：透明包装（`BaseFilter`）逐项继承内层能力；变换型包装
（TLS 等）默认不继承，必须逐项确认后自己声明。
**现有实现应声明的能力**：TCP / Unix 流（各驱动）：`HalfClose`、`ReadTimeout`、`WriteTimeout`、`IdleTimeout`、`ResumableAfterCancel`；
`memoryStreamPair`：`HalfClose`、`AnyThread`、`ResumableAfterCancel`（超时不支持，调用即报错）。
**一致性套件**：独立产物 `io-testkit`，抽象测试基类，实现方提供"建立一对连通的流"的工厂与（可选）"让对端 RST"的钩子并声明能力；必选契约
全部运行，可选能力按声明运行。运行对象：TCP（io_uring / epoll / kqueue / poll / IOCP / WSAPoll）、Unix 域套接字、`memoryStreamPair`、`BaseFilter`。

### 28.7 第二个协议消费者：`http` 仓库（首版复刻 hyper 的 HTTP/1.1 能力）
用户："第一个版本就是按照他们的实现 100% 复刻能力，充分利用我们 neton.io 的底层能力统一定义、统一抽象方式、统一并发模型，基于 Kotlin Native 的特点去
落地这些库，每个库都要独立仓库和独立的 spec 文档。" 因此修订 3 中"冻结的 HTTP/1.1 子集"不再作为范围：
- 第二个消费者是独立仓库 `http`（坐标 `com.netonstream:http`，通用类型在 `neton.http`、HTTP/1.1 专属实现在 `neton.http.h1`），首版复刻参考实现
  `http` 1.5.0、`httparse` 1.10.1、`hyper` 1.11.1 的全部 HTTP/1.1 能力（含原子集排除的 `Expect: 100-continue` 与 Upgrade / CONNECT）；详细规格在
  `http/SPEC.md`，以参考源码盘点出的能力清单为准（§28.14）。
- 修订 2 / 3 中冻结的消息边界规则（TE + CL、重复 / 非法 CL、截断、chunk / trailer 上限、HEAD / 1xx / 204 / 304、未读完请求体的复用上限、1xx 后继续等待
  最终响应、上传内存上限等）**作为安全基线**移入 `http/SPEC.md`：hyper 的行为与基线一致或更严格时照 hyper；不一致之处在 `http/SPEC.md` 逐条记录
  并决定（默认取两者中更安全的一方，可配置时默认值也取更安全的一方），不得默默采用任何一方。
- 对 neton-io 的意义不变：只用公开 API（不引用 `ReactorResumer`）；实现中每一处"只用公开 API 做不到"的地方记入本 SPEC（缺口 → 设计 → 修复）。

### 28.8 可观测性
- **更新**：计数器按反应器存放，只在所属反应器上用普通字段更新（热路径无原子操作）。
- **读取**：其他线程不得直接读这些字段。快照由 `suspend fun TcpServerGroup.stats(): ServerStats` 取得：向每个反应器投递一个复制计数器的任务
  （§28.3 的投递规则），各反应器的快照各自在一个时刻上一致，合计值由不同时刻的快照相加（文档写明：不是全局原子快照）。`ServerStats.isFinal`：
  反应器处于 `RUNNING` / `DRAINING` 时快照为非最终（`false`，停机中的数字仍在变化）；反应器进入 `CLOSING` 时在最后一次排空后发布最终计数，之后的
  `stats()` 返回它（`true`）。
- **指标**：`activeConnections`（已交给 handler 的连接）与 `acceptReservations`（accept 循环预留的配额）分开；已接受 / 已关闭总数；每反应器：
  循环轮数、执行任务数、每轮最多任务数、各恢复环与外部队列的当前长度与峰值、被拒绝的投递数、阻塞等待次数、读写字节数；调度延迟：按 1/1024
  采样记录"投递 → 开始执行"的时间，对数分桶直方图（采样开关默认关闭，关闭时零开销）；缓冲池占用（缓存的数组数与字节数）。
- **分配数不由 neton-io 提供**：Kotlin/Native 没有按线程的分配计数，每请求分配数只以 callgrind 实测为准（`CustomAllocator::Allocate`
  调用者统计），GC 次数与堆大小由 `GcStats` 提供；不以源码中的对象清单代替实测。

### 28.9 数据报层：只列需求，暂不实现
QUIC / HTTP/3 需要，不套用 `IoStream`（需保留报文边界与对端地址）。有明确的 QUIC 消费方时另起 SPEC。需求：`DatagramSocket` 绑定 / 连接、
`send(to)` / `receive(from)` 保留边界、截断报告（`MSG_TRUNC`）、缓冲所有权与 §28.6 一致、发送侧 EAGAIN 背压、取消 / 关闭语义同 §28.6；批量
（`recvmmsg` / `sendmmsg`、io_uring `RECVMSG` / `SENDMSG` 含 multishot、Windows `WSARecvFrom` / `WSASendTo`、Apple）；QUIC 所需的可选能力：
GSO / GRO、ECN 位、`IP_PKTINFO` / `IPV6_RECVPKTINFO`、双栈、PMTU 相关选项。QUIC 协议本身、加密、拥塞控制不在 neton-io。

### 28.10 当前不做
不扩大池化；不默认绑核；不实现 UDP；不在核心加入 HTTP / TLS。

### 28.11 执行顺序（连续实施；每一步单独验证、单独提交、SPEC 记录结果）
0. §28.13 产物更名：构建与发布配置改为 `com.netonstream:io`（包名不变）、testkit 坐标 `com.netonstream:io-testkit`、msgtrans 依赖随之更新（发布在 0.2.0 时进行）。
1. §28.3 反应器生命周期（`DRAINING` / `CLOSING` 条件）与 `ReactorResumer` 接受 / 拒绝、`dispatch` 拒绝语义 + msgtrans 适配 `Boolean` 返回值；验收：§28.3
   测试 (a)–(e) + msgtrans §15.1 两种模式。
2. §28.6 契约落地：能力声明、并发与线程规则、io_uring / IOCP 关闭时在收割结果后才唤醒；`io-testkit` 一致性套件在所有流实现上运行。
3. §28.8 最小计数（§28.4 所需）+ `echo-client-mass` 开环 / 混合模式 → §28.4 的 L1 / L2 / F2 / F3；不达标项按候选逐个改并测量。
4. §28.12 读取前准入 → §28.4 的 L3。
5. §28.5 Windows 实跑（依赖用户提供 Actions 结果或令牌；可与上面各步并行）。
6. §28.7 `com.netonstream:http`（`neton.http` + `neton.http.h1`）；缺口清单逐项回到 neton-io。
7. §28.8 可观测性 API 完整版。
8. §28.9 数据报层：有 QUIC 消费方时另起 SPEC。

### 28.12 读取前准入
`limitInFlight`（§27.3）在 `call(req)` 内等待许可：请求已被读取、解码、成对象后才排队，过载时内存与排队都不受它约束。补上读取之前的准入。
修订 2 的"解码前获取、随后可等待网络数据"会让不发数据的空闲连接占满许可，撤回；本版把连接的一次服务分成两个阶段：
- **等待输入阶段（不持有许可）**：连接等待下一个请求的首字节。只受读缓冲容量（每连接一个读缓冲）与空闲超时约束，不占许可。空闲连接永远不持有许可。
- **请求处理阶段（持有许可）**：读缓冲中出现下一个请求的至少一个字节后才获取许可；获取后继续读取与解码该请求（受帧读取速率约束，见下）、调用 service、
  把响应交给写路径。
- `class Admission(permits: Int, acquireTimeoutMillis: Long)`（kotlinx `Semaphore`），由应用创建、可被多个服务共享；`serve(framed, service, admission)`
  与 `Framed` 的服务循环接受它。**使用 `Admission` 时必须配置帧读取速率**（否则构造报错）：持有许可后等待同一请求的其余字节的时间因此有上限
  （默认首字节起 10 s 内收完一帧），发了一个字节就停下的连接不能无限期占住许可。
- **许可不足时**：该连接停止解码与读取（已缓冲的字节留在读缓冲里，读缓冲不增长），TCP 接收窗口随之关闭，背压到达客户端。等待超过
  `acquireTimeoutMillis` → 该连接以错误关闭（`AdmissionTimeoutException` 记入统计），使过载时的排队有期限。
- **释放**：响应交给写路径（`feed`）后释放；处理中抛异常、连接关闭、协程取消时在 `finally` 中释放（恰好一次）。等待中被取消的获取不占用许可。
- **输出另有上限**：许可释放不代表输出资源已释放。每连接的待发送字节受 `Framed` 写缓冲高水位约束（默认 64 KiB，超过即 flush，flush 挂起直到内核接收）；
  另设每连接输出上限（默认 256 KiB，含一个超大帧），超过时 `feed` 挂起。全服务的输出内存上限 = 每连接输出上限 × `maxConnections`。
- **等待队列上限**：每条连接同一时刻至多等待一个许可，因此等待者数 ≤ 连接数；**只有 `maxConnections` > 0 时才有界**，`maxConnections = 0` 时文档写明
  等待者数不受限。
- **许可耗尽时停止的是**：解码与读取；accept 仍按 `maxConnections` 进行，不因许可耗尽而暂停（两者正交，文档写明组合方式）。
- **与 `limitInFlight` 的关系**：可同时使用；`Admission` 约束"读入并处理中的请求数"，`limitInFlight` 约束"执行中的调用数"。
- **测试**：(a) 许可 2：先建立 2 条不发数据的连接，再有一条连接发送真实请求——真实请求被处理（空闲连接不占许可）；(b) 许可 2：两条连接各发一个字节后
  停下，第三条连接的真实请求在帧读取速率期限后得到处理，停下的两条被关闭；(c) 许可 2、10 条连接各发大量请求：同时处理中的请求 ≤ 2，许可耗尽期间
  服务端读缓冲不增长、客户端写入最终阻塞；(d) 等待超过 `acquireTimeoutMillis` 的连接被关闭并计数；(e) 取消等待中的连接不漏许可；(f) 输出上限：客户端
  不读响应时服务端每连接待发送字节不超过上限。§28.4 L3 以它为准入手段。msgtrans 的 `maxInFlightRequests` 保持现状，另评估是否改用 `Admission`。

### 28.13 产物坐标、包名、仓库与框架集成（2026-09-27 用户确定；同日修订）
用户："现在先不用考虑 neton.http 的包冲突问题，io / http / quic / websocket 这个目前和 neton 框架是并行的，等 io / http / quic / websocket 性能无敌的时候
再考虑如何调整 neton 框架去接入。" 本节只确认命名与集成边界，**不代表 §28 修订 3 的生命周期与准入设计已通过技术终审**。

| 职责 | Maven 坐标 | Kotlin 包 |
|---|---|---|
| I/O 与网络底座 | `com.netonstream:io`（testkit：`com.netonstream:io-testkit`） | `neton.io.*`（不变） |
| HTTP 协议库（§28.7 起步），仓库 `http` | `com.netonstream:http`（通用 + HTTP/1.1 + HTTP/2）；提议另发 `com.netonstream:http3`（HTTP/3，依赖 `quic`），同一仓库，待评审 | `neton.http`（通用的请求、响应、客户端、服务端等类型）；协议专属实现在 `neton.http.h1` / `neton.http.h2` / `neton.http.h3` |
| WebSocket 协议库，仓库 `websocket` | `com.netonstream:websocket` | `neton.websocket` |
| QUIC 协议库，仓库 `quic` | `com.netonstream:quic` | `neton.quic`（依赖 §28.9 数据报层） |
| msgtrans | `com.netonstream:msgtrans`（不变） | `msgtrans.*`（不变） |
- **仓库**：底座仍名 `neton-io`，不随坐标缩短；协议库仓库与坐标同名：`http`、`websocket`、`quic`（本地在 `~/projects/PulseKit/` 下与 `neton-io` 并列；
  GitHub 远程仓库待用户确认组织与可见性后再建）。
- **发布**：`com.netonstream:neton-io:0.1.0` 保持不动；~~自 0.2.0 起用新坐标~~ 新坐标 `com.netonstream:io` 是全新的版本线，从 0.1.0 起（2026-09-29 发布，用户确认）；旧坐标发布一次 Maven 重定位（relocation）POM 指向新坐标（未做）。
- **包的划分按职责**：通用 HTTP API 放在 `neton.http`，使用者切换协议版本不必更换整套类型；只有协议专属的实现与扩展放进 `h1` / `h2` / `h3`。
- **与现有 Neton 框架并行，集成延期**：`io` / `http` / `quic` / `websocket` 是独立建设的协议栈，按自身职责设计 API，**不为现有框架（`com.netonstream:neton-http`，
  `neton.http.*`）避让包名或类型名**，也不要求与它同时链接。等这套协议栈经过验证、性能达到目标后，再由框架调整去接入（届时处理两者的包与类型关系）。
- **目标表述**：在协议正确、取消安全、资源有界、调度公平的前提下，持续对标领先实现（geario 等）提升性能。"性能无敌"是方向，不是验收标准；每个阶段
  都要有明确的对照场景与数据（§28.4 的规程与验收指标）。

### 28.14 协议库的建设方法（2026-09-27 用户确定）
- **参考实现固定版本**：本地学习副本在 `~/projects/reference/rust/`（浅克隆到发布 tag，只读；索引与提交号见其中 `README.md`）：`http` 1.5.0、`hyper` 1.11.1、
  `httparse` 1.10.1、`h2` 0.4.19、`tungstenite` 0.30.0、`tokio-tungstenite` 0.30.0、`quinn` 0.11.12 / `quinn-proto` 0.11.18 / `quinn-udp`、`h3` 0.0.8；
  自有性能参考 `~/projects/Neton/geario`、`geario-http`。升级参考版本是单独的一步，另行记录。
- **首版范围 = 能力对等**：每个库的首版复刻对应参考实现的全部能力（配置项、协议行为、错误语义、限值与默认值）；各库 `SPEC.md` 以参考源码逐项盘点
  出的能力清单作为对等清单，逐项标注"已实现 / 有意不同（附理由）/ 不适用（附理由）"。
- **统一在 neton.io 之上**：字节流一律是 `IoStream`（及 `Framed` / `Codec`），数据报一律是 §28.9 的数据报层；并发模型一律是 neton-io 的协程 + 反应器
  （连接归属所属反应器、取消按 §28.3 / §28.6、准入按 §28.12、资源上限与停机按 §23.4 / §27）；运行时相关的抽象（执行器、计时器、I/O trait）不照搬参考实现的
  trait 层级，而是映射到 neton-io 的对应能力。协议状态机尽量不依赖真实网络与系统时间（像 `quinn-proto` 那样输入字节 / 时间 / 事件、输出字节 / 事件），
  以便确定性测试，但不强求所有协议套用同一种接口。
- **按 Kotlin / Native 落地**：不逐字翻译 Rust 的 `Poll`、生命周期参数与 trait 层级；API 用挂起函数与结构化并发；热路径遵守零分配规则（§24、§26.1），
  以 callgrind 实测为准。
- **每个库独立仓库、独立 SPEC**：`http`、`websocket`、`quic` 各自的 `SPEC.md` 规定该库的全部内容；neton-io SPEC 只记录底座缺口。
- **测试**：移植参考实现的测试套件（按其用例逐条对应）与模糊测试目标；外部一致性套件（HTTP/2：h2spec；WebSocket：Autobahn；QUIC：quic-interop-runner）
  纳入各库验收。
- **性能对照**：同机同负载、**同等功能配置**对照参考实现（hyper / h2 / tungstenite / quinn）与 geario / geario-http，按 §28.4 的规程与验收指标；不以简化路径
  对比对方完整实现宣布胜出；参考实现也不是"已证明最快"的排行榜。
- **许可与署名**：参考实现为 MIT 或 MIT / Apache-2.0 双许可；各库以 Apache-2.0 发布，`NOTICE` 保留上游版权声明与 MIT 许可正文（已建立）。
- **顺序**：HTTP/1.1 → WebSocket → HTTP/2 → QUIC → HTTP/3（QUIC 依赖 §28.9 数据报层，届时另起 neton-io SPEC）。

### 28.15 协议库 SPEC 提出的底座缺口（2026-09-27，随三份 SPEC 草案汇总）
各库草案：`~/projects/PulseKit/http/SPEC.md`、`websocket/SPEC.md`、`quic/SPEC.md`（本地仓库，远程待建）。对 neton-io 的要求：
| 缺口 | 提出方 | 处理 |
|---|---|---|
| 数据报层（13 项：非阻塞与就绪、批量接收、每数据报 ECN、PKTINFO 源 / 目的 IP、DF / PMTU 探测与 `mayFragment`、Linux GSO / GRO、Windows USO / URO、双栈与 v4 映射、EINVAL 回退、容忍 EMSGSIZE / ECONNRESET、套接字缓冲、重绑定） | quic §9 | §28.9 从"只列需求"升级为另起数据报层 SPEC（以 `quinn-udp` 的逐平台设施与 8 个测试为依据）；实施排在 QUIC 之前 |
| 计时精度：1 ms 粒度，pacing 需亚毫秒 | quic §9 | 反应器已有两套计时：10 ms 计时轮（只给流超时，§23.2）与按截止时间的纳秒最小堆（`Reactor.kt:159`，`delay` / `withTimeout` 走它）；等待时截止时间向上取整到毫秒作轮询超时（epoll_wait 只收毫秒）。**先审计**公开接口、各驱动轮询超时的精度（epoll_pwait2 / io_uring TIMEOUT / kqueue 可到纳秒）与实测调度延迟，再决定补什么；不另造计时器 |
| 公开的单调时钟与系统时间 | quic §9 | 公开为 API |
| 加密安全的随机数 | http（HeaderMap 防碰撞）、websocket（掩码、key）、quic（连接 ID、令牌、重置密钥） | 三方共用，评估放入 neton-io（平台 CSPRNG：`arc4random_buf` / `getrandom` / `BCryptGenRandom`，I/O 平台层的一部分）或单独的小模块 |
| 读取前准入 | http（服务端可选） | §28.12 |
| 升级后交出"读缓冲剩余 + 原流" | http、websocket | 协议库内以 `IoStream` 包装实现，无需改 neton-io |
TLS 1.3（QUIC 所需的 12 项能力见 quic §4）不在 neton-io：2026-09-27 用户确定由另外封装的 `openssl-kotlin`（基于 OpenSSL 4.0.2）提供；在它可用之前，只做与 TLS 无关的工作。

**§28.11 第 0 步完成（2026-09-27）**：模块改名 `:io`（目录仍为 `neton-io/`），坐标 `com.netonstream:io`，版本 `0.2.0-SNAPSHOT`；CI 与 README 改用 `:io:` 任务；
msgtrans 依赖改为 `com.netonstream:io`、版本 `0.2.0-SNAPSHOT`；neton-io / msgtrans / pulsekit 三个工程在 macOS 上编译通过；POM 为 `com.netonstream:io:0.2.0-SNAPSHOT`。
Gradle 任务路径从 `:neton-io:…` 变为 `:io:…`。旧坐标的重定位 POM 在发布 0.2.0 时生成。

**§28.11 第 1 步完成（2026-09-27）**：`Reactor` 增加生命周期（`readyToStop`：根作业结束后继续循环，直到本地无任务、内核无在途操作；随后以 CAS 把外部入口
换成哨兵、执行此前已接受的投递、进入 STOPPED）；驱动钩子 `inFlightKernelOps` / `cancelInFlightOps`（io_uring、IOCP 以 `liveOps` 实现）；三个驱动的循环改由
`readyToStop` 决定退出；`dispatch` 在入口关闭后抛 `ReactorStoppedException`，另有不抛异常的 `tryDispatchExternal`；`ReactorResumer.resume` /
`resumeWithException` 返回 `Boolean`（其他线程上改走 `tryDispatchExternal`）。
实施中发现并处理：新语义下，停机过程中向已经退出的工作反应器投递会被拒绝（以前是静默丢失）。
- `cancelConnections`（强制停机）改用 `tryRunOn`：被拒绝说明该反应器已无连接，无事可做。
- `ReactorGroup.stop()` 的唤醒改用 `tryDispatchExternal`：已在关闭的反应器不需要唤醒；此前一次偶发失败即来自这里的竞争。
- `postToReactor`（取消 / 清理时切回反应器）被拒绝时忽略：关闭中的反应器已无本作用域的挂起操作。
- 交接连接的 `runOn` 保持抛异常：服务期间反应器不可能已停止，被拒绝即是缺陷。
测试 `ReactorLifecycleTest`：(a) 排空期间跨线程结果、名字解析、取消通知都送达；(b) 进入 CLOSING 时在途内核操作为 0；(c) 300 个反应器生命周期内约 22 万次外部投递与关闭
竞争，接受数 = 执行数（把入口哨兵临时去掉时测试挂起，被强制结束）；(d) 所属线程上 `resume` 排队、不在调用处执行；(e) 槽位约定下 1 万次取消与恢复竞争，每次恰好一次。
msgtrans：`ReactorQueue` 对被拒绝的恢复关闭队列并报告；`MSGTRANS_REACTOR_RESUMER=0` 关闭快速路径（msgtrans §15.1）。
验证：neton-io macOS 117/117（2 次），colima Linux arm64 io_uring / multishot / epoll 各 119/119（各 3 次）；msgtrans macOS 与 Linux io_uring / epoll 在
`MSGTRANS_REACTOR_RESUMER` = 1 / 0 下均 44/44；mingwX64、Android、iOS 编译通过。

**§28.11 第 2 步完成（2026-09-27）**：§28.6 契约落地。
- `IoStream.capabilities`（`StreamCapability`：HalfClose / ReadTimeout / WriteTimeout / IdleTimeout / AnyThread / ResumableAfterCancel）；未声明的能力
  调用即失败：`shutdownOutput` 默认抛 `UnsupportedOperationException`，非零超时经 `requireTimeoutCapabilities` 拒绝（0 = 不设超时，总是接受）。
  套接字流声明 HalfClose + 三种超时 + ResumableAfterCancel；内存流声明 HalfClose + AnyThread + ResumableAfterCancel；`BaseFilter` 透传。
  `Framed` 设置 `readRate` 时要求 ReadTimeout。
- 同一流同时至多一个读、一个写：三个驱动与内存流在入口检查，第二个并发调用抛 `IllegalStateException`（io_uring / IOCP 以 `readBusy` / `writeBusy` 实现，槽位释放时清除）。
- io_uring / IOCP：`closeStream` 不再立即唤醒挂起操作，只设置中止原因并请求取消；收割到 CQE / 完成包（内核不再使用缓冲区）后才以 `ClosedException` 唤醒。
- 新模块 `io-testkit`（`com.netonstream:io-testkit`）：`IoStreamConformance` 一致性套件，17 项检查，逐项独立建流、限时、汇总失败。必选：读追加且不返回 0；
  写完全部并清空 src；写返回后 src 可复用；FIN 先数据后 -1；写已关闭的对端得 `IoException`；关闭幂等、挂起操作与关闭后调用得 `ClosedException`；
  并发第二读 / 第二写被拒；writev 保序；任意切分重组一致；被取消的大写入 src 前进量 = 对端收到量；RST 为 `IoException` 而非 EOF（可选工厂）。
  按能力：已声明的必须可用，未声明的必须被拒绝（HalfClose、ReadTimeout 及超时后可继续、Write / Idle 超时、取消后可继续否则关闭、跨线程写）。
- 运行对象：TCP（含 linger 0 的 RST 工厂）、Unix 套接字、`memoryStreamPair`、`BaseFilter`(TCP)、`BaseFilter`(内存)；另有一项确认套件能发现违约（读返回 0）。
  反向验证：临时去掉内存流的并发读检查，memory 与 filter(memory) 两项失败。CI 测试任务同时运行 `:io-testkit:`。
验证：macOS kqueue 与 poll 驱动 6/6；colima Linux arm64 io_uring（multishot）/ io_uring 单次 RECV（`NETON_IO_URING_MULTISHOT=0`）/ epoll 各 6/6（各 3 次）；
`:io` 全量在单次 RECV 配置下 119/119（第 1 步记录的 "multishot" 与 "io_uring" 两组实际都是默认的 multishot，单次 RECV 路径此次补测）；
mingwX64（含测试编译）、iOS、Android 编译通过。Windows 上的运行结果待 `ci/windows-validation`。

**§28.11 第 3 步（2026-09-28）：§28.4 的 F2 / L2 / L1 / F3**。脚本与原始输出在 `bench/s28-4/`；153 上服务端 `taskset 0,1` 两个反应器、负载发生器
`taskset 2,3`，每格轮换、预热 3 s 后测 30 s。
- **最小计数**（`NETON_IO_STATS=1`，默认不计）：`plain_tasks_run`、`budget_exhausted`（预算用完仍有任务的轮数）、`max_resume_queued` / `max_plain_queued`
  （每轮开始时的峰值）、`external_batches` / `external_tasks` / `max_external_batch`、`refused_posts`（入口关闭后被拒的投递，原子计数，只在拒绝路径上）。
- **负载发生器 `echo-client-mass`**：按类配置连接（`--class 名称:连接数:深度:速率`），速率 > 0 为开环（按固定时刻、随机相位发送一批，延迟从
  **计划时刻**起算），速率 0 为闭环；每类报告延迟分位（附样本数）、发生器自身的发送滞后、每连接完成数（Jain、最小、中位）、相邻两次完成的最长间隔；
  `gen_cpu` 为发生器在所占核上的利用率（≥ 0.8 该轮作废）。实施中修正了发生器自身的公平性缺陷：一条连接在一次处理中可以不停地读写（闭环深度
  1 万时整轮独占线程），改为每次最多 8 次读写、剩余工作下一轮再处理。
- **F2（恢复环持续繁忙，进程内探针 `fairnessProbe`）**：1024 对协程经 `ReactorResumer` 不停交接，每轮预算全部被恢复环占满。
  严格优先（原实现）：计时器迟到 p99 21–31 µs；**跨线程任务与新连接进入 handler 30 s 内一次也没有执行**（10 轮全部如此）——普通任务被饿死。
  实测还发现：只要**一对**协程经恢复环交接，普通任务就永远排不上（环里始终有一项）。
  改为**轮转**：两类都有工作时普通任务与恢复环一对一交替（`drainTasks`；`NETON_IO_RING_PRIORITY=strict` 保留原行为作对照）。
  结果（epoll / io_uring 各 5 轮，1 万样本）：计时器迟到 p99 21–33 µs、最大 ≤ 5.4 ms；跨线程等待 p99 ≤ 29 µs、最大 ≤ 1.5 ms；accept → handler p99 ≤ 10 µs。全部达标。
  代价（cachegrind，每请求指令数，epoll / io_uring × 原始回显 / 按行回显）：与上一提交相比 −0.0% 至 +0.2%，在测量噪声内；采纳为默认。
  回归测试 `FairnessTest.resumeRingHandoffsDoNotStarveOrdinaryTasks`：512 对交接中新协程与跨线程恢复仍能执行；以严格优先运行时该测试失败。
- **L2（饱和公平，闭环 1000 冷 + 64 热（深度 16））**：所有服务端配置、所有轮次：每类 Jain ≥ 0.974（neton 按行 ≥ 0.998），最小 / 中位 ≥ 0.72，最长完成
  间隔 ≤ 35 ms，冷连接 p99 为"64 热换成 64 冷"时的约 1.3 倍。**达标**。服务端 CPU 1.94–1.98 核（饱和）。
  有效性：原始回显（neton 与 geario）各轮发生器利用率 0.92–0.99，按规程作废——本机 2 核发生器不足以压满它们，**本机无法给出与 geario 的有效吞吐对照**；
  按行回显（服务端更慢）大多有效。饱和吞吐 `C`（有效轮中位）：neton epoll 按行 491k、io_uring 按行 430k；原始回显 603k、geario 665k 只是下限。
  轮转与严格优先的吞吐配对（有效轮）：+0.8% / −0.4% / −6.3%，同一配置各轮本身相差 ±10%，吞吐配对无法分辨；以上面的每请求指令数为准。
- **L1（未饱和，开环：1000 冷 × 20 req/s + 64 热（深度 16）共 0.5 C）**：冷连接 p99 与冷连接单独运行（1.16 ms，其中约 1.05 ms 是发生器按毫秒计时的发送滞后）相比：
  neton 原始回显 1.24 ms（1.07 倍）、geario 1.24 ms——**达标**；neton 按行 epoll 5.42 ms、io_uring 5.06 ms（4.4–4.7 倍）——**不达标**。最大延迟 ≤ 15.5 ms，无错误。
  原因（逐项排除）：关闭 GC（`gc=noop`）后按行模式 p99 1.20 ms，即全部来自 GC；单线程标记无改善。GC 统计：每秒约 13 次，标记暂停最大 0.4 ms，但
  **到达安全点的时间**合计 0.66 s / 31 s、单次最大 7–9 ms——GC 协调线程以 `sched_yield` 自旋等待各反应器到达安全点，反应器占满所绑的核时让出无效，
  一次 GC 让所有线程停几毫秒。分配来自按行编解码（每个请求一个 `String` 及其 UTF-8 转换），不来自 neton-io 的路径（原始回显不分配、不受影响）。
  应用侧措施（一个变量）：`GcTuning.setMinHeap`（`NETON_IO_GC_MIN_HEAP_MB`）64 MiB → GC 49 次 / 31 s，p99 1.31–1.35 ms（1.15 倍，**达标**）；256 MiB →
  12 次，p99 1.24–1.28 ms，但 p999 11–13 ms（每次 GC 的停顿仍在）。结论：库自身路径满足 L1；按请求分配的应用须设置最小堆，并尽量不在请求路径上分配。
  库仍不设置进程级 GC 参数（§26.8）。
- **按行模式的开销**（cachegrind）：每请求约 1.79 万条指令，原始回显约 2400；其中 66% 在 Kotlin/Native 运行时的 UTF-8 转换（`createStringFromUTF8`
  24%、`to_string` 22%、`validate_next` 20%），约每字节 150 条指令往返。`Framed` 与反应器只占几个百分点。对协议库的约束：热路径不把载荷转成
  `String`；需要文本时按字节解析、ASCII 快速路径。
- **F3（L1 热连接背景 + 100 冷 × 20 req/s + 1 条连接每 100 ms 一次写入 1 万个帧）**：冷连接 p99 与去掉该连接时相比：neton 按行 epoll 1.11 倍、io_uring
  1.18 倍、原始回显 1.01 倍、geario 1.01 倍——**全部达标**（按行模式的基线本身受上面的 GC 影响）。
  说明：首轮 F3 让该连接闭环不停写入 1 万帧，原始回显时它以约 25.7M 帧/秒把发生器的一个线程占满（发送滞后 62 ms），结果反映的是发生器而非服务端，
  作废；改为开环每 100 ms 一批（"一次写入 1 万个小帧"，总负载约 0.7 C）。另有一轮（geario F3 第 2 轮）因误在测试进行中运行了一次默认参数的负载工具
  而作废，只用其余 4 轮。
- **规模**：以上只证明 2 反应器、4 vCPU 虚拟机的结果（§28.4 规模说明）。

**§28.11 第 4 步（2026-09-28）：§28.12 读取前准入与 L3**。
- `Admission(permits, acquireTimeoutMillis)`（`kotlinx.coroutines.sync.Semaphore`，可跨反应器共享）；`Framed.serveLoop(admission, handler)` 与
  `serve(framed, service, admission)`：等待输入时不持有许可；读缓冲里出现下一个请求的字节后才获取；获取不到时停止读取（已缓冲的留在读缓冲，
  读缓冲不增长），等待前先把已编码的响应写出；响应交给写路径后即释放，异常 / 关闭 / 取消时在 `finally` 中恰好释放一次；等待超过期限抛
  `AdmissionTimeoutException`（`timeouts` 计数），连接关闭。未配置帧读取速率时拒绝使用。无争用路径 `tryAcquire`，不分配；争用路径记录等待
  次数与对数分桶的等待时间（`waits`、`waitQuantileMicros`）。输出上限沿用 `Framed` 的写缓冲高水位（64 KiB，超过即 flush 并挂起到内核接收）：
  每连接待发送字节 ≤ 64 KiB + 一个帧。
- `serveTcp(..., maxConnections)`（原先只有 `listenGroup` 有）；echo 服务端：`NETON_IO_MAX_CONNECTIONS`、`NETON_IO_ADMISSION`（许可数）、
  `NETON_IO_ADMISSION_WAIT_MS`、`NETON_IO_ECHO_DELAY_MS`。负载发生器：`--reconnect 1`（服务端关闭后 10 ms 重连，未答复的请求计为拒绝）、
  `--send-deadline-ms`、`--timeline 1`（每秒完成数）。
- 测试 `AdmissionTest`（§28.12 (a)–(f) + 未配置帧读取速率被拒绝），7/7。反向验证（每次改一处）：许可在等待输入前获取 → (a)(c) 失败；去掉 `finally`
  释放 → (b)(e) 失败；不获取许可 → (b)(c)(d) 失败。反向验证中发现 §27.11 的缺陷（失败的测试挂起 / 中止进程），已先行修正。
- **L3**（修订后的设计见 §28.4 "L3 修订"）：服务每请求挂起 1 ms、许可 64、等待期限 250 ms、`maxConnections` 2000；`C`（闭环 3 次中位）epoll 36.7k、
  io_uring 44.5k；过载 2 C 开环 30 s，各 3 轮：服务端许可等待 p99 ≤ 33 ms、最大 ≤ 66 ms（≤ 250 ms，且与 1064 条连接 / 64 许可 × 约 2 ms 的估计相符，
  因此没有超时）；RSS 10 → 40–42 MB（声明上限：缓冲池 2 × 4 MiB + 2000 ×（每连接固定开销按 4 KiB 估计、未单独测量 + 64 KiB 读 + 64 KiB 写）≈ 266 MiB；实测增长约 30 MB，远在其内）；无错误、无崩溃；过载
  结束后第 1 秒即回到 C 的 93%–100%（epoll 34.3k–35.4k / 36.7k，io_uring 44.3k–45.0k / 44.5k）。**全部达标**。
  客户端观测：非流水线的冷连接在 2 C 下 p99 7–9 ms（按连接先到先得地分到许可）；流水线热连接 p99 约 19–20 s——它们自己的请求堆在内核套接字缓冲里
  （本机回环缓冲自动增长到数 MB，250 ms 发送期限没有丢弃任何请求，因为请求都已写进内核），这正是上面修订第 2 条所说的、服务端无法约束的部分。
  首次 L3（回显不挂起）未形成过载、准入从未等待，结果保留在 `bench/s28-4/l3.out` 作为修订依据。


**§28.15 加密安全随机数（2026-09-28）**：`neton.io.core.secureRandom(dst, offset, length)` / `secureRandomLong()`，取操作系统的 CSPRNG：
Apple `arc4random_buf`；Linux 与 Android 读 `/dev/urandom`（每进程一个描述符；`getrandom` 比这些目标所用的 Linux sysroot 与 Android API 级别新）；
Windows `BCryptGenRandom`。http（HeaderMap 防碰撞密钥）、websocket（掩码与 key）、quic（连接 ID、令牌）共用。测试：只写指定范围、两次不同、1 MiB 的
字节频率无明显偏差、范围检查；macOS 127/127，colima Linux io_uring / epoll 各 129/129。

**§28.13 发布修复（2026-09-28）**：0.1.0 之后加入的代码使发布路径失效（普通目标构建不编译共享源集，因此一直没有暴露）：(1) 亲和性的
`cpu_set_t` 全局变量被 cinterop 导出，`commonizeCInterop` 失败；(2) `posixMain` / `epollMain` 共享元数据中直接调用宽度或有无符号因平台而异的
接口（`size_t` / `ssize_t` / `socklen_t` / `long` / `nfds_t` / `pthread_t`、`iovec` 字段），编译失败（29 + 3 处）。修正：这些调用改经 `posixshim`
中定宽的 C 包装（`neton_recv` / `neton_send` / `neton_sendv`（在 C 中组装 iovec，至多 64 块）/ `neton_accept` / `neton_get|setsockopt_int` /
`neton_setsockopt_linger` / `neton_cpu_count` / `neton_read` / `neton_poll` / `neton_thread_id` / 管道单字节读写）；签名中不出现平台结构体。
验证：全部 9 个共享源集的元数据编译通过；12 个目标编译通过；`publishToMavenLocal`（io 与 io-testkit 全部目标）成功；macOS 127/127，colima Linux
io_uring（multishot / 单次 RECV）/ epoll / poll 各 129/129；热路径代价（cachegrind，原始回显每请求指令）epoll 2391 → 2390、io_uring 3764 → 3766，
在噪声内。

**§28.6 补充（2026-09-28，接入 TLS 流时）**：
- 一致性套件新增参数 `orderlyClose`（默认 `close()`）：`close()` 无法发出协议结束标记的流（TLS 的 close_notify 需要写入，而 `close()` 不挂起）
  以 `shutdownOutput(); close()` 作有序结束。未声明 `ResumableAfterCancel` 的流，"被取消的写"检查改为：对端收到的字节不多于 `src` 的前移量、
  且是其前缀，对端可能得到错误而非 EOF（取消可能发生在一条消息中间）；"取消后不可继续则关闭"检查不再先向对端写入（对端已关闭时写入本应失败）。
- `neton.io.core.intResult(n)` 公开（原为反应器内部）：`IoStream` 实现与包装流在读写路径上 `return intResult(n)`，返回字节数不再每次装箱一个
  `Int`（-128..127 之外）。TLS 流实测每请求分配由 4 次降为 2 次。

## 29. 数据报层（UDP）与计时精度审计（2026-09-28，草案，待评审）

§28.9 只列了需求；QUIC 首版需要它（quic SPEC §9），本节把它定为规格。依据：`quinn-udp` 0.11（`~/projects/reference/rust/quinn-0.11.12/quinn-udp`，
`src/unix.rs`、`windows.rs`、`lib.rs`、`tests/tests.rs`）。原则同 neton-io：零分配热路径、反应器集成、各平台差异收在 C 包装里（§28.13 的教训：
平台结构体与宽度可变的类型不进入共享源集）。

### 29.1 API（`neton.io.net`）
- `suspend fun bindUdp(address: SocketAddress, options: UdpOptions = UdpOptions.Default): UdpSocket`（必须在反应器内调用；双栈：绑定 `::` 时接受 v4
  映射地址）。
- `class UdpSocket`：
  - `suspend fun recv(batch: RecvBatch): Int`：一次最多收 `batch.capacity` 个数据报（Linux `recvmmsg`，其余平台 1 个），返回个数；无数据时挂起到可读。
    `RecvBatch` 由调用方创建并复用：一块连续缓冲 + 每个数据报的 `RecvMeta`（来源地址、长度、GRO 步长 `stride`、ECN、目的 IP），元数据用原始字段
    存放，**每次接收不分配**。
  - `suspend fun send(transmit: Transmit)`：`Transmit`（目的地址、ECN、内容区间、`segmentSize`（GSO）、源 IP）由调用方复用；发送缓冲满时挂起到
    可写。`fun trySend(transmit): Boolean` 不挂起的版本。
  - 查询：`maxGsoSegments`（运行中可能降为 1）、`groSegments`、`mayFragment`（平台不支持禁止分片时为 true）、`localAddress`。
  - `setSendBufferSize` / `setReceiveBufferSize` 及查询；`close()`（挂起的收发得到 `ClosedException`）。
- 同一套接字同时至多一个 `recv`、一个 `send`（同 §28.6）；所属反应器线程使用。
- 重绑定（换网络）：由上层（QUIC 端点）新建套接字并替换；本层不提供"原地换地址"。

### 29.2 平台设施（对照 `quinn-udp`）
| 能力 | Linux / Android | Apple | Windows |
|---|---|---|---|
| 批量接收 | `recvmmsg`，每次 32 | `recvmsg` 1 个（`recvmsg_x` 为后续性能项） | `WSARecvMsg` 1 个 |
| 批量发送 / 分段 | `sendmsg` + `UDP_SEGMENT`（GSO，至多 64 段；探测失败为 1） | 无（1 段） | `UDP_SEND_MSG_SIZE`（USO，可选） |
| 接收合并 | `UDP_GRO`（尽力开启，得到步长） | 无 | `UDP_RECV_MAX_COALESCED_SIZE`（URO，可选） |
| ECN 读 | `IP_RECVTOS` / `IPV6_RECVTCLASS` | 同左；**双栈套接字上 v4 不支持 `IP_RECVTOS`**（参考注释） | `IP_ECN` / `IPV6_ECN` |
| ECN 写 | `IP_TOS` / `IPV6_TCLASS` 控制消息 | 同左 | 同左 |
| 目的 IP / 源 IP 选择 | `IP_PKTINFO` / `IPV6_RECVPKTINFO` | `IP_RECVDSTADDR`（无 `IP_SENDSRCADDR`）、`IPV6_RECVPKTINFO` | `IP_PKTINFO` / `IPV6_PKTINFO` |
| 禁止分片（PMTU 探测） | `IP_MTU_DISCOVER = IP_PMTUDISC_PROBE`、`IPV6_MTU_DISCOVER` | `IP_DONTFRAG`、`IPV6_DONTFRAG` | `IP_DONTFRAGMENT` / `IPV6_DONTFRAG` |
- 设置失败的选项不致命：记录为能力缺失（如 `mayFragment = true`、GRO 步长 1），与参考一致。

### 29.3 错误处理（对照 `unix.rs`）
- `EMSGSIZE`：MTU 探测时预期出现，发送视为完成（不抛出）。
- 使用 GSO 时的 `EIO` / `EINVAL`：运行中把 `maxGsoSegments` 降为 1（驱动或网卡不支持），本次报错由上层重试。
- 第一次 `EINVAL`（带 ECN / TOS 控制消息被拒绝的平台）：记下，之后不再附带 TOS，参考的回退。
- `ECONNRESET` / `ECONNREFUSED`（ICMP 不可达，Windows 与 Linux 的已连接套接字）：接收侧忽略，继续收。
- 其他错误以 `IoException` 抛出。

### 29.4 反应器集成
- 读写就绪沿用现有驱动：epoll / kqueue / poll 的就绪等待；io_uring 驱动先以 `POLL_ADD` 等就绪再做 `recvmmsg` / `sendmsg`（批量系统调用的收益保留；
  是否改为 `IORING_OP_RECVMSG` 多次提交作为后续性能项，以 callgrind 与吞吐实测决定）；IOCP 用 `WSARecvMsg` 重叠操作。
- 控制消息（cmsg）的组装与解析全部在 C 包装中完成，Kotlin 只看到定宽字段。

### 29.5 测试（`quinn-udp` 的 8 个测试逐个对应，另加）
`basic`、`basic_src_ip`、`ecn_v6`、`ecn_v4`、`ecn_v6_dualstack`、`ecn_v4_mapped_v6`、`gso`（Linux）、`socket_buffers`；另加：批量接收 32 个、
GRO 步长拆分、`EMSGSIZE` 容忍、GSO 运行中降级（以故障注入模拟 `EIO`）、关闭时挂起的收发得到 `ClosedException`、每次收发零分配（callgrind）。

### 29.6 计时精度审计（quic SPEC §9：QUIC 计时粒度 1 ms、pacing 需要亚毫秒）
- 现状：`delay` / `withTimeout` 走纳秒最小堆，但等待时把截止时间向上取整到毫秒作为轮询超时（epoll_wait / poll 只收毫秒）；流超时走 10 ms 计时轮。
- 审计内容：各驱动的轮询超时精度（epoll_wait 毫秒、`epoll_pwait2` 纳秒（Linux 5.11+）、io_uring `TIMEOUT` 纳秒、kqueue 纳秒）；实测 `delay(d)`（d 为
  0.2 ms、1 ms、5 ms）在各驱动上的迟到分布（p50 / p99 / 最大），空载与 §28.4 L1 负载下各一次。
- 决策规则：若 1 ms 截止时间的 p99 迟到 ≤ 1 ms，只公开单调时钟 API；否则在该驱动上改用纳秒超时（`epoll_pwait2` 或 io_uring `TIMEOUT`），以同样的
  测量验收；不另造计时器。
- 同时公开单调时钟（`monotonicNanos()`）与系统时间（`systemTimeMillis()`）。

**§29 首版实施（2026-09-28）**：`bindUdp` / `UdpSocket`（`recv(RecvBatch)`、`send` / `trySend(Transmit)`、`maxGsoSegments` / `groSegments` /
`mayFragment`、缓冲大小、`close`）、`SocketAddress`（公开的 IP 地址值）、`EcnCodepoint`。`RecvBatch` / `Transmit` 由调用方复用，数组在其生命期内
固定一次，收发不分配。套接字设置、cmsg 组装与解析、`recvmmsg`（32）/ `recvmsg`、GSO 探测（内核 ≥ 4.18 且测试套接字接受 `UDP_SEGMENT` → 64）、
GRO、PMTU 选项全部在 `posixshim` 的 C 包装中，签名只含定宽类型与 `void *`（§28.13）。反应器增加 `awaitReadable` / `awaitWritable`（就绪驱动用
一次性关注，io_uring 用 `POLL_ADD`）。错误按 §29.3：`EMSGSIZE` 视为已发送；GSO 下 `EIO` / `EINVAL` 把 `maxGsoSegments` 降为 1；第一次
`EINVAL` 之后不再附带 IPv4 的 TOS；接收侧跳过 `ECONNREFUSED` / `ECONNRESET`。Windows：未实现（IOCP 与 WSAPoll 两种驱动都以
`UnsupportedOperationException` 明确拒绝），待有 Windows 主机时实施 `WSARecvMsg` / `WSASendMsg`。
测试（`posixTest/UdpTest`）：quinn-udp 的 8 个测试逐个移植（basic、basic_src_ip、ecn_v6、ecn_v4、ecn_v6_dualstack、ecn_v4_mapped_v6、gso、
socket_buffers，断言同参考：分段内容、来源端口、来源 / 目的地址（v4 映射规范化）、ECN），另加一次收满 32 个、`EMSGSIZE` 不报错、关闭唤醒挂起的
接收，共 11 个。macOS 11/11（GSO / GRO 为 1，批量 1）；colima Linux arm64 io_uring / epoll / poll 各 11/11（GSO 64、GRO 64、一次 `recvmmsg` 收到
32 个）；全量 macOS 138/138、Linux 三种驱动配置各 140/140；9 个共享源集元数据与 12 个目标编译通过。每次收发零分配的 callgrind 验证与吞吐对照
（quinn-udp）作为后续项。

**§29.6 审计结果（2026-09-28，153，`timerProbe`，每项 2000 次，反应器绑定一个核）**：`delay(d)` 的迟到（µs）：
| 驱动 | d | 空载 p50 / p99 / 最大 | 负载（同核有原始回显服务端在跑）p50 / p99 / 最大 |
|---|---|---|---|
| epoll | 1 ms | 72 / 89 / 1479 | 66 / 81 / 3099 |
| epoll | 5 ms | 75 / 111 / 355 | 71 / 88 / 129 |
| io_uring | 1 ms | 23 / 43 / 134 | 16 / 30 / 3239 |
| io_uring | 5 ms | 27 / 56 / 202 | 22 / 39 / 3376 |
| poll | 1 ms | 72 / 98 / 785 | — |
没有提前触发。按决策规则（1 ms 截止时间 p99 迟到 ≤ 1 ms）：各驱动都满足，不改用纳秒等待；公开 `monotonicNanos()` / `systemTimeMillis()`
（`neton.io.core`）。负载下偶见 3 ms 的最大值来自与服务端共用一个核。参考栈的对照：quinn 运行其上的 tokio 计时轮精度为 1 ms。

### 29.7 Windows（2026-10-08，补全 §29.2 的 Windows 列）
此前 `Udp.mingw.kt` 的每个函数都抛 `UnsupportedOperationException`（IOCP 的注释称 WSAPoll 驱动支持 UDP，并不属实），quic 与 http3 因此不能在
Windows 上运行。按 `quinn-udp` 0.11 `windows.rs` 补全，C 部分在 `winshim.def`，边界与 `posixshim` 相同（定宽字段，错误在调用内取得，§33.5）。
- **套接字**：`socket(…, SOCK_DGRAM, IPPROTO_UDP)`、非阻塞、`IPV6_V6ONLY` 按选项、绑定。设置（参考的 `UdpSocketState::new`，每项必需）：经
  `SIO_GET_EXTENSION_FUNCTION_POINTER` 取 `WSARecvMsg`；承载 IPv4 时 `IP_DONTFRAGMENT`、`IP_PKTINFO`、`IP_RECVECN`；IPv6 套接字 `IPV6_DONTFRAG`、
  `IPV6_PKTINFO`、`IPV6_RECVECN`。`mayFragment` 为 false。
- ⚖️ **双栈与 IPv4 选项**：参考以"v4 套接字或非 v6only"判断是否承载 IPv4；Rust 标准库在 Windows 上保留 `IPV6_V6ONLY` 开启（系统默认），而本库默认
  双栈，于是绑定 `::1` 的套接字也被设 IPv4 选项，Windows 以 WSAEINVAL 拒绝（CI：`ecnV6`、`ecnV6Dualstack`）。改为：只有绑定在 `::` 或 v4 映射
  地址上的双栈套接字才承载 IPv4。
- **接收**：`WSARecvMsg` 一次一个数据报；控制消息 `IP_PKTINFO` / `IPV6_PKTINFO`（目的 IP）、`IP_ECN` / `IPV6_ECN`（C int）、`UDP_COALESCED_INFO`
  （URO 步长）。URO 与参考一样默认不开（quinn issue 2041），`groSegments` 仍报 64（参考的 `gro_segments`，用作接收缓冲倍数）。
- **发送**：`WSASendMsg`；源 IP 以 `IP_PKTINFO` / `IPV6_PKTINFO`，ECN 以 `IP_ECN`（IPv4 或 v4 映射目的）/ `IPV6_ECN`，`segmentSize < length` 时附
  `UDP_SEND_MSG_SIZE`（USO）。USO 能力在另一个测试套接字上探测（作为套接字选项会让每次发送都分段）：接受则 512 段（参考："empirically found on
  Windows 11 x64"），否则 1。⚖️ 第一次 WSAEINVAL 后不再附带 ECN（与 §29.3 的 EINVAL 回退一致；参考在不支持发送 ECN 的系统上每次发送都失败）。
- **错误码**：WSAEWOULDBLOCK → 挂起；WSAEMSGSIZE → 视为已发送（§29.3）；WSAEINVAL → §29.3 的回退；WSAECONNRESET / WSAENETRESET（ICMP 端口
  不可达在下一次接收时报告）→ 忽略、继续收。
- **mingw-w64 头文件**缺 `IP_ECN`、`IP_RECVECN`、`IPV6_ECN`、`IPV6_RECVECN`（50）、`UDP_SEND_MSG_SIZE`（2）、`UDP_COALESCED_INFO`（3）：按 Windows SDK
  的值定义（与 windows-sys 0.60 核对）。
- **WSAPoll 驱动**：就绪等待沿用现有 `awaitReadable` / `awaitWritable`。
- **IOCP 驱动**：数据报的收发仍是上面的非阻塞调用；可读等待用 0 字节、`MSG_PEEK` 的重叠 `WSARecv`——有数据报排队时完成且不取走数据（libuv
  对 UDP 的 zero read）；该操作以 WSAEMSGSIZE（数据报放不进 0 字节）或 WSAECONNRESET / WSAENETRESET 结束同样表示"接收不会阻塞"。可写等待：
  UDP 发送只在发送缓冲满时阻塞，极少见，以 WSAPoll 零超时检查加短退避（同 `awaitConnect`）。关闭时，被 `closeStream` 结束的操作一律报
  `ClosedException`：`closesocket` 可能先于取消完成，完成包这时带 WSAENOTSOCK（CI：`closeWakesParkedRecv`），此前被报成 IoException。
- **测试**：`UdpTest` 由 posixTest 移到 nativeTest，两个 Windows 驱动都运行 11 个（GSO 测试的 `Transmit` 按内容分配：128 × 512 字节比默认容量多
  1 字节）。Windows 上 `UdpTest.gso` 为 maxGsoSegments = 512、groSegments = 64。

### 29.8 Windows 的计时精度（2026-10-08，缺陷，已修复）
- **现象**：quic 在 Windows CI 上的吞吐约为 Linux / macOS 的五分之一（`FairnessTest` 重连接 5 MiB/s 对 23–24 MiB），发送预算几乎从不用满（让出 0–2
  次，Linux 145 次）：发送端大部分时间在等计时器。§29.6 的审计只在 Linux 上做过。
- **测量**（新测试 `TimerPrecisionTest`：`delay(1)` 200 次，按中位数断言迟到 < 4 ms，同时打印 p99 / 最大）：Windows IOCP 迟到 p50 12,887 µs、
  p99 15,768 µs；WSAPoll p50 14,857 µs、p99 15,393 µs——Windows 默认 15.6 ms 的系统时钟周期，`GetQueuedCompletionStatusEx` 与 `WSAPoll` 的毫秒超时
  都按它取整。同一轮 Linux epoll p50 72 µs、io_uring 34 µs，macOS kqueue 303 µs。
- **修正**：每个 reactor 运行期间以 `timeBeginPeriod(1)` 把计时精度提到 1 ms，关闭时 `timeEndPeriod(1)`（Windows 对调用计数；Windows 10 2004 起
  只影响本进程）；两个驱动都如此（`winshim.def`，链接 winmm）。
- **结果**：Windows IOCP 迟到 p50 1,051 µs、p99 2,050 µs；WSAPoll p50 999 µs、p99 2,013 µs；CI 11 项全部通过。剩下的约 1 ms 来自截止时间先向上
  取整到毫秒、再加 1 ms 的系统精度，与 tokio 计时轮的 1 ms 同级；若要亚毫秒，后续可改用高精度可等待计时器（`CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`）
  接入完成端口，以同一测试验收。

### 29.9 Windows 的批量接收（2026-10-08，性能）
- **现象**（quic SPEC §11.13 遗留）：quic 在 Windows 上传 4 MiB 需 0.55–0.85 s（Linux 0.10–0.17 s），发送预算几乎不让出。两端在同一反应器线程时
  （`TransferProbeTest`，调试构建）RTT 达 90–245 ms，接收轮几乎每个数据报就因时间预算让出一次（4,296 个消息 4,179 次）：接收侧每次系统调用只取一个
  数据报，接收方每处理两个包就回一个 ACK，发送方随之处理更多 ACK。
- **测量**（新测试 `UdpReceiveProbeTest`，`NETON_IO_UDP_PROBE=1`，CI 作业 `udp probe`：100 个数据报一批发出、全部到达后再收，接收不挂起，得到的就是
  接收路径本身的开销）：每个数据报的接收 IOCP 13.7 µs、WSAPoll 23–33 µs；同一次 C 调用（`WSARecvMsg`）在批量时不到 1 µs，开销主要在每次调用的 Kotlin
  一侧。Linux（`recvmmsg`，32 个一批）0.3–2.5 µs。
- **修正** ⚖️：Windows 一次接收调用最多取 32 个数据报——shim 内对 `WSARecvMsg` 循环到套接字为空或批满，依次写入 `RecvBatch` 的各个槽位，与 Linux
  `recvmmsg` 填批的方式相同（Winsock 没有批量接收）；第一个就失败时返回该错误（无数据为 WSAEWOULDBLOCK），之后的错误结束本批、下次调用再见到。
  quinn-udp 在 Windows 上每次一个（BATCH_SIZE 1）。`NETON_IO_UDP_BATCH`（1–64）可覆盖，供对照测量。
- **结果**（同一 CI 轮，批 32 对批 1）：
  - io 探针每个数据报的接收：IOCP 0.86–0.89 µs 对 13.7 µs，WSAPoll 2.3–2.7 µs 对 23–33 µs。
  - quic `TransferProbeTest`（4 MiB，5 轮，以 io main 构建）：WSAPoll 146–154 ms 对 436–895 ms，IOCP 310–329 ms 对 447–847 ms；RTT 由 90–245 ms 降到
    1.5–2.6 ms，接收让出由数千次降到 137–152 次。同一轮 Linux epoll 212–222 ms（另一台机器）。
- **Apple 同样处理**：macOS 也是每次一个数据报（CI 上 RTT 50–150 ms，与 Windows 批 1 相同的形态）。posixshim 非 Linux 分支同样循环 `recvmsg`，
  默认 32 个（quinn-udp 只有开启 `fast-apple-datapath` 时才批量，用私有的 `recvmsg_x`）。本机 macOS（M 系列）：io 探针每个数据报 6.1 → 1.6 µs；
  quic `TransferProbeTest` 186–277 → 147–157 ms，RTT 30–50 ms → 约 0.9 ms。quic 全量（以本版 io 构建）两种 TLS 模式各 615 个通过。
- `UdpTest.socketBuffers` 原以 `BATCH_SIZE > 1` 判断"Linux 会把缓冲加倍"，改为按操作系统判断。
- **仍待做**：IOCP 比 WSAPoll 慢一倍（310 对 150 ms），另行剖析。

## 30. TCP 连接的地址（2026-09-29，Neton 框架引擎适配器提出的缺口）

需求：框架的请求对象带 `remoteAddress`（按 IP 限流、访问日志），而底座没有取 TCP 流对端地址的办法，适配器只能把所有客户端记为 "unknown"。
对照：tokio `TcpStream::peer_addr` / `local_addr`。

- API（`neton.io.net`）：`val IoStream.peerAddress: SocketAddress?`、`val IoStream.localAddress: SocketAddress?`（getpeername / getsockname）。
  返回 null 的情形：不是套接字的流（内存流）、包装流（TLS 流等，应在包装之前从原始套接字取）、Unix 套接字、流已关闭（先查关闭标志，
  避免 fd 被复用后返回另一条连接的地址）、对端已不在（`ENOTCONN`）。每次调用做一次系统调用、分配一个 `SocketAddress`，调用方在 `accept`
  之后取一次保存。双栈监听上的 IPv4 客户端报告为 v4 映射地址，`toCanonical()` 取回 IPv4。
- 实现：POSIX 在 `posixshim` 的 `neton_sock_addr`（复用 `neton_sa_read`）；Windows 解析 Winsock 的 `sockaddr` 字节（族小端在 0，端口大端在 2，
  IPv4 在 4..8，IPv6 在 8..24，scope 在 24..28）。
- `SocketAddress.ipString()`：只取 IP 的文本，同 Rust `IpAddr` 的 Display（IPv6 按 RFC 5952 压缩最长的零组、相同长度取第一段，v4 映射写成
  `::ffff:a.b.c.d`，有 scope 时带 `%scope`）；框架的 `remoteAddress` 只要 IP。`toString()` 不变。
- 测试（`nativeTest/PeerAddressTest`，6 个）：IPv4 与 IPv6 两端互为对端 / 本端、双栈监听的映射地址、内存流为 null、关闭后为 null、
  `ipString` 对照 Rust std 的 `Ipv6Addr` 显示测试。
  macOS 全量 152/152；153 epoll 与 io_uring 各 154/154；mingwX64 编译通过，Windows 实跑待 `ci/windows-validation`。

## 31. 容器 CPU 亲和性与默认 reactor 数（2026-09-30）

状态：本地实现，功能测试通过；对 Arena 的性能收益未测。未发布。

- Linux / Android 上 `cpuCount()` 数调用线程 `sched_getaffinity` 掩码中的 CPU，不再用宿主机的 `_SC_NPROCESSORS_ONLN`。
  不连续的掩码按置位数计，不按最大 CPU 编号。
- 每次调用都重新读掩码（不缓存）。需要沿用原始可用 CPU 集时，在给线程绑核之前查询。显式给出的 `reactors` 始终优先。
- 内核 CPU 数超过初始缓冲区时 `EINVAL`，缓冲区加倍重试；查询失败时退回在线 CPU 数，至少为 1。
- 不推算 cgroup v1 / v2 的 CPU 带宽配额：只有配额限制的容器仍须显式设置。要支持配额，须解析进程实际的 cgroup 挂载与路径以及
  祖先层级的限制，只读 `/sys/fs/cgroup/cpu.max` 不够。
- 不涉及监听组共享、GC 设置、准入、取消或 HTTP 语义。
- `bench/test-cpu-count.py` 编译生产用的 shim，测试限制、继承、首尾 CPU 掩码与恢复；`--source-ref` 编译旧 shim 作反向对照。
  Linux 的 `CpuCountTest` 覆盖公开的 Kotlin 入口，包括在 taskset 下运行。
- 发布门槛：I/O 测试，加上在 Linux 上用 Arena 的压测端对完整 Neton 业务路径做不改其他条件的 A/B，记录压测端、原始请求、
  驱动、cpuset 与连接数。只有功能测试不能说明吞吐有提升，也不能把它称作已确认的 Arena 回归修复。

验证（2026-09-30）：macOS arm64 153/153；Linux arm64（Colima，2 CPU）epoll、io_uring、multishot 各 156/156。公开的 Kotlin
`CpuCountTest` 在 `taskset -c 0` 下也通过。C 测试覆盖真实的限制 / 继承 / 恢复，以及注入的稀疏掩码、CPU 编号 2048、`EINVAL`
扩容与系统调用失败时的回退。用修复前 HEAD 的 shim，同一测试在单 CPU 断言处中止。未回滚任何源码。

### 31.1 Arena 证据与后续性能门槛

来源：https://github.com/MDA2AV/HttpArena/actions/runs/36611334152（PR 1517）。runner 报告 128 个可用 CPU；baseline 声明的服务端
cpuset 有 64 个逻辑 CPU。日志没有直接列出服务端的 reactor 线程或 GC 暂停。

| 引擎 | 最好 RPS | 最好一轮的 CPU 百分比 | 三轮的 p99 |
| --- | ---: | ---: | --- |
| NetonStream | 372848 | 3386.0 | 138.30–142.40 ms |
| Hyper4k | 1034982 | 5282.7 | 6.04–8.66 ms |

这些是性能症状，不能证明根因是 GC、竞争或驱动。用 CPU / RPS 估算的每请求成本不是恒定值。诊断构建的 malloc 次数与 GC 清扫
对象数不能混用来推断或排除 GC 饱和。NetonStream 最好一轮中第一种请求模板约占完成数的 22.8%，Hyper4k 为 32.9%；本地三种模板
等量的测试有参考价值，但复现不了这种完成比例，也复现不了 Arena 压测端的调度。

后续受控步骤（保持相同的框架 / 业务路径，以已发布的 Hyper4k 作对照）：
1. 在 Linux 上对比修复前与只含亲和性修复的构建。记录实际可用 CPU 列表、每个监听组的 reactor 线程数、内核 / 驱动、产物哈希、
   错误以及全部轮次（不只是最好的一轮）。
2. 在相同 reactor 数下，对比显式的 `NETON_IO_DRIVER=epoll` 与 `iouring`。macOS kqueue 的测量隔离不出 Linux 驱动的行为；
   自动回退不得悄悄改变这项实验。GC 与准入设置保持不变。
3. 在单独的诊断轮次里采集逐线程 CPU、调度等待与 GC 安全点 / 暂停数据。只有这些观测支持时，才测试监听组共享或 GC 调优。
4. HTTP 断连监视与头部 Map 的分配优化分开测量，保留取消、半关闭、流水线、背压与流式的一致性测试。

按当前粗略的每请求 CPU 成本，仅仅用满 64 个 CPU，NetonStream 约为 0.70M RPS，Hyper4k 约为 1.25M（推算，未测）。在这个 CPU 预算下
要达到 3M，还须把每请求 CPU 降到约 21 µs；只调 reactor 数不够。

### 31.2 第一轮本地完整 Neton 测试（2026-09-30）

作为 CPU 数量的正确性修复保留，**不是**已证明的吞吐提升。本轮没有发布 Maven，也没有改 Arena PR。

- 未改动的 Arena beta21 条目的普通 Linux arm64 release 构建：修复前用已发布的依赖；修复后只把 io 换成带此亲和性补丁的版本。
  io-v0.1.0 标签到本地基线之间只改了文档 / POM 链接。HTTP 仍是已发布的 0.1.0。
- Mac 上共享的 Colima，两个虚拟 CPU，服务端绑 CPU 0，wrk 绑 CPU 1，显式 io_uring，两个非 TLS 监听端口，GET / Content-Length
  POST / chunked POST 混合。三组交替、每轮 10 秒的测量，另有带响应校验的预热。复现不了 Arena 的独立压测端、硬件、64 CPU 掩码
  与四个监听端口。
- 256 连接时六轮都没有 socket / 状态 / 超时错误。RPS 中位数修复前 79245、修复后 75705（约 −4.5%），范围重叠。进程每请求 CPU
  中位数 12.58 对 13.22 µs。这些观测不能说明有提速。进程线程数 9 对 7，与两个监听组都不再多开 reactor 一致。
- 第一次 4096 连接的尝试继承了 1024 的 fd 软上限而失败。压测工具现在把自身及子进程的软上限提到 65536（不超过硬上限），并记录
  两个上限。失败的输出保留，不覆盖。
- fd 上限修正后，六轮 4096 连接仍全部有请求超时（修复前 680 / 654 / 698；修复后 674 / 675 / 685）。不得把它们的 RPS 当作通过的
  性能结果，不得推断超时原因，也不得把本地失败等同于 Arena 的回归。没有放宽任何超时。高连接数下的准入 / 调度另行调查（结果见 §32）。
- 两个独立的 HTTP 延迟监视原型因 io_uring 下倒退被否决，协议源码中都未保留。见 http SPEC 的否决实验记录。
- 复现与原始证据：同级目录 `bench-arena/linux-watch-ab.py`、`affinity-ab-builds.json` 与 `results/affinity-iouring-*`。每一轮都带
  产物哈希、请求数、错误数、CPU 测量窗口与客户端 CPU 占用。新旧两个二进制都保留。仍需多核 Linux / Arena 验证。

## 32. io_uring 下 accept 每轮只取一个连接（2026-10-01，缺陷，已修复）

现象（§31.2 的 4096 连接超时）：本地 Linux（Colima，服务端 1 CPU，wrk 1 线程）先用 4096 连接预热 3 秒，再用 4096 个新连接测量，
io_uring 下每轮 600–900 个请求超时，epoll 下为 0；预热后等 10 秒再测也为 0。内核计数器（ListenOverflows、ListenDrops、
SyncookiesSent 等）全为 0，不是监听队列溢出。

定位：测量中每秒读监听 socket 的 Recv-Q（accept 队列长度）：第 1 秒 3494–3686，之后每秒只少约 100；t=4 s 与 t=7 s 两次快照之间
`bytes_received` 不变的连接有 2764–2836 个，它们的 Recv-Q 恰好是一个请求的字节数（60 / 82 / 101）——握手已完成、还在 accept 队列里，
第一个请求从未被读。原因：`UringReactor.accept` 每次都提交一个 ACCEPT SQE 并挂起，恢复要等下一轮循环的 enter 与收割，所以
**每轮循环最多取走一个连接**；有负载时每轮要处理几百个请求，accept 速率被压到每秒约 100。就绪型 reactor 的 `accept` 一直是先直接
`accept()`，EAGAIN 才等可读，一次能取空队列。不预热时连接是在服务端空闲时建立的，很快被取完，所以看不出来；Arena 的压测端同样
在开局一次建立 4096 个连接。

修复：`UringReactor.accept` 先对非阻塞的监听 socket 直接 `accept()`；队列空（或出错）时才提交 ACCEPT SQE 等待，错误仍由 SQE 的
结果报告。Handoff 与 ReusePort 两种接受方式都经过这里。

测试（`nativeTest/AcceptBacklogTest`）：100 个连接完成握手、排在 accept 队列里（不超过 macOS `kern.ipc.somaxconn` 的 128），一个只做
`yield()` 的协程计数；取走这 100 个连接期间它运行的次数必须少于 10。每轮循环最多运行它 256 次（任务预算），旧实现每个连接一轮：
修复前 io_uring 为 25,498 次（300 个连接时 76,498）而失败，epoll 通过；修复后两者都通过。全量：macOS 154/154；Linux arm64（Colima）
io_uring、io_uring multishot、epoll 各 157/157；mingwX64、linuxX64、androidNativeArm64 编译通过。

效果（同上环境，完整 Arena neton 条目，只把 io 换成本修复，预热后 4096 连接，各 2 轮）：accept 队列在第 1 / 5 / 9 秒都是 0（修复前约
3500 → 2700）；3 秒无进展的连接 0（修复前约 2800）；超时 0（修复前 683 / 883）。RPS 在同一范围（服务端只有 1 CPU）；p50 从约 16 ms
升到约 57 ms，因为现在 4096 个连接全都在接受服务（4096 / 55k ≈ 74 ms 的平均排队），而修复前卡在 accept 队列里的连接不计入延迟。
对 Arena 的收益仍须在多核 Linux 上测。探测脚本：`bench-arena/probe-4096.sh`（`WARM=1 SNAP=1`）。

## 33. 让 CI 在全部平台通过（2026-10-07 / 08，缺陷，已修复）

背景：io 的 GitHub CI（编译 + macOS kqueue / poll、Linux epoll / poll / io_uring、Windows IOCP / WSAPoll）此前从未全部通过，0.1.1 与 0.2.0
都是在 CI 失败的状态下发布的；JVM 目标的测试不在 CI 中。本节逐项记录原因与修正。每项都由 CI 的失败或本机的对照实验确认，修正后在 CI 上复验。

### 33.1 IOCP：按 fd 取下标前先扩容
`arr[ix(fd)]` 先读出数组字段、再调用会扩容替换该数组的 `ix()`，第一个超出初始容量（256 / 4）的 socket 在 accept 中抛
ArrayIndexOutOfBoundsException 并使进程退出（CI 第 64 个测试后崩溃）。9 处改为 `ix(fd).let { arr[it] }`。

### 33.2 测试与小缺陷
- `TcpListener.localAddress` 改走 TCP 路径的 `getsockname`（原用的 UDP 辅助函数在 Windows 上抛异常）；JVM 的监听器也报告本地地址。
- IOCP accept：监听 socket 在挂起的 AcceptEx 下被关闭（WSAENOTSOCK / WSAEINVAL）报 ClosedException（与非重叠 accept 一致）；连接在 AcceptEx
  完成前被客户端重置或中止（ERROR_NETNAME_DELETED、ERROR_CONNECTION_ABORTED、WSAECONNRESET、WSAECONNABORTED）只丢弃该连接、继续 accept，
  不再让监听器失败。
- IOCP 公平轮转：关闭流时，被推迟到下一轮的读者以 ClosedException 失败（与就绪型 reactor 一致）；此前它会在已关闭、可能被内核复用的 fd 号上
  recv。推迟的读者改为按 fd 记录。
- `io_uring_setup` 对 ENOMEM / EAGAIN 以退避重试（最多 2 s）：ring 的内存在关闭后异步释放，连续创建 reactor 时会短暂失败（CI）；只有 EINVAL
  才退回普通 ring，并报告原始 errno。
- 测试：Windows 上对已关闭的 Unix socket 对端写一次可能先成功；400 连接测试改为依次建立（macOS 监听队列上限 128）再全部并发使用；
  `BufferPoolTest.parkedReadHoldsNoPooledArray` 对 IOCP 与 io_uring 一样豁免（重叠读在挂起期间拥有数组）。
- CI 新增 `test-jvm`（ubuntu-latest、macos-14、windows-latest 上运行 `:io:jvmTest`）。

### 33.3 Windows 的写：背压与取消
- **整块接收**：缓冲区未满时，Windows 的非阻塞 `send()` 和重叠 `WSASend` 都会一次接收整块数据（32 MiB 也一次完成），写永远不挂起，写超时与
  取消无从发生，慢对端的内核内存没有上限。修正：每次调用最多交出 `MAX_SEND_CHUNK`（256 KiB），WSAPoll 的 `send` / `WSASend`（向量）与 IOCP
  都如此；缓冲区满后下一次调用得到 WSAEWOULDBLOCK（或挂起），写者停住。
- **取消不精确**（IOCP）：限块后 `cancelledWriteAccountsExactlyWhatWasSent` 失败：对端在第 786432 字节（3 × 256 KiB）处与源数据不同。CI 诊断
  打印：前 2 块已完成（sent = 524288），第 3 块的 WSASend 被取消后完成包报告 n = 0、err = 995（ERROR_OPERATION_ABORTED），而对端共收到
  33,816,576 字节——比 32 MiB 多整一块：被"取消"的 256 KiB 实际已全部发出，接手的写者又发了一遍。Windows 无法精确取消挂起的 TCP 发送，
  所以 IOCP 不能按完成包里的字节数实现 §28.6 的"取消的写准确计算已发送字节"。
- **修正（暂存发送）**：IOCP 的写先做非阻塞 `send()`（缓冲区有空间时直接进入内核，不额外复制，这是常见路径）；返回 WSAEWOULDBLOCK 时，
  把下一块（不超过 256 KiB）复制进 reactor 拥有的内存（malloc），同时从调用者的缓冲区消费掉——交给内核即计为已发送——再从这份副本发起
  重叠 WSASend 并等待。这个发送从不为写者取消：写者被取消或超时时立即恢复，发送自行完成或随 socket 关闭结束（关闭时取消，完成包到达后
  释放副本）。每个 socket 至多一个：下一次写先等它完成，保证顺序、内存上限为一块。复制只发生在背压时（此时吞吐受对端限制）。
  `writev` 相同（非阻塞 WSASend 不超过 256 KiB，阻塞时把各缓冲区的前 256 KiB 复制进一块）。

### 33.4 reactor 的活性：根作业结束后外部投递不能让它停不下来
- **现象**：CI 上 IOCP 在 `ReactorLifecycleTest.externalPostsRacingCloseAreRunOrRefused` 挂住 25 分钟（逐测试日志定位）。
- **原因**：根作业结束后，`readyToStop` 要等本地队列为空才进入 CLOSING；而每轮循环开头都会吸收外部队列。测试的投递线程把积压保持在 10,000
  左右，每轮只执行任务预算（256）个，本地队列永远不空，reactor 永远不停。POSIX 上每次投递都写一次唤醒管道（一次系统调用），投递者被拖慢；
  IOCP 的唤醒有 `wakePending` 的 CAS 挡住多余的投递包，投递几乎无成本。
- **对照实验**（本机 macOS，同一测试各 2 次）：原版通过（0.3 s）；只把唤醒改成同样的 CAS 保护，两次都在 120 s 超时——与 IOCP 相同的挂起；
  再加上修正，3 次均通过（每次约 455 万次投递）。
- **修正**：根作业结束后，只在本地队列为空时才吸收外部队列（`absorbExternal`）。本地队列在有限轮内排空，`readyToStop` 看到空队列即关闭入口，
  其间已接受的投递在 CLOSING 时恰好执行一次（语义不变：已接受的必执行，入口关闭后拒绝）。全量：macOS kqueue、poll 各 154 / 154。

### 33.5 Windows：错误码必须在调用内取得
- **现象**：服务端打印 `read failed: Winsock error 0` 并关闭连接，客户端读回显时得到 WSAECONNABORTED（WSAPoll，`MultiReactorTest`）；
  非阻塞 connect 报 `connect ... failed: Winsock error 0`（IOCP，`TcpEchoTest.manyConcurrentConnections`）。
- **原因**：`recv` / `send` / `connect` 等失败后，在 Kotlin 里再调用 `WSAGetLastError()` 时线程的最后错误可能已被清零：从外部调用返回时，运行时
  会做自己的 Win32 调用（例如成功的 `TlsGetValue` 会把最后错误置 0）。connect 的 WSAEWOULDBLOCK 因此被读成 0，被当作失败；accept 只需等待时
  同样会让监听器失败。此前（2ba9fcd）把读取提前到失败处仍在 Kotlin 侧，不够。
- **修正**：`winshim.def` 中的 `neton_recv_nb`、`neton_send_nb`、`neton_sendv_nb`（非重叠 WSASend）、`neton_accept_nb`、`neton_connect_err`、
  `neton_bind_err`、`neton_listen_err` 在 C 中调用后立即读取 `WSAGetLastError()` 并返回（读写返回 −错误码）；IOCP 的重叠操作本来就在 C 中
  读取（`neton_start_result`）。只用于报错信息的 `socket()` / `getsockname` 未改。

### 33.6 结果
运行 37653522928（815f593）：编译 + 链接全部目标、macOS kqueue / poll、Linux epoll / poll / io_uring、Windows IOCP / WSAPoll、JVM（Linux、macOS、
Windows）共 11 项全部通过，是 io 的 CI 第一次全部通过。提交本节后的运行作为第二次确认（结果见提交说明）。
