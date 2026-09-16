## Context

统一执行内核（`crates/arkflow-core/src/executor/`）以 chain 为单位运行 Job：chain 内任务融合进单个 event loop，chain 间通过有界 flume 通道（容量 1024）传递 `Envelope`（`Data(MessageBatchRef, Arc<dyn Ack>)` / `Barrier` / `Watermark` / `Eos`），严格 FIFO。checkpoint barrier 沿边随数据流动，多输入顶点用 `Aligner`（`barrier.rs`）对齐；source 链在注入 barrier 前 drain 在途 ack 并 seal `CommitFrontier`（`task.rs:440-540`、`commit.rs`）。Hub–Agent 控制面只承载命令/心跳/上报，没有数据通道。

放置层因传输缺失被迫按连通分量整体落位（`job.rs:706`），`graph.rs:462` 对跨节点拆边硬报错。设计参考对象是本地 Arroyo 代码库（`/Users/chenquan/code/rust/arroyo`）的 `arroyo-worker/src/network_manager.rs`——生产验证过的 Rust 网络数据面，与本内核的 Envelope FIFO + barrier 搭车模型同构。

## Goals / Non-Goals

**Goals:**

- Envelope 四变体（含 barrier/watermark）跨节点传输，语义与进程内 flume 边不可区分：FIFO、有界、保序。
- `input-durability` 契约跨节点保持：源偏移只随回执齐备的连续 ack 推进，下游崩溃不产生已 ack 丢失。
- 远程边失败 fail-closed：断链使 attempt 失败，交由既有 generation fencing 重新放置。
- 全部语义可用 loopback transport（同进程双端）离线测试。

**Non-Goals:**

- 放置算法放开、跨节点 checkpoint 完成、rescale、exactly-once sink 提交（各属后续 change）。
- 数据面鉴权、断线续传、跨节点 error/late-route 旁路边（v1 由 placement 约束同节点）。

## Decisions

**D1：线协议 = 裸 TCP + 定长帧头 + Arrow IPC，不用 gRPC/Arrow Flight。**
帧头沿用 Arroyo 布局（`network_manager.rs:104`）：`{src_op: u32, src_subtask: u32, dst_op: u32, dst_subtask: u32, len: u32, kind: u32}`，`kind ∈ {Data, Signal, Receipt}`（第三种为本设计新增）。Data 负载 = Arrow IPC `IpcDataGenerator::encoded_batch`（含 dictionary tracker，字典增量只传一次）；Signal 负载 = serde 序列化的 `Barrier/Watermark/Eos`；Receipt 负载 = `{kind: Acked|Held|Released, seq: u64}`。`MessageBatchRef = Arc<MessageBatch>`（`lib.rs:150`）取内层 `RecordBatch` 编码，`__meta_` 元数据列随行过线（下游本就按元数据列设计）。备选 tonic gRPC 因控制消息需侧信道、帧层多一跳而被否决；Arrow Flight 亦然。零新增依赖。

**D2：接线 = 远程端点替代，不做 Arroyo 式全图中继；路由规则按帧类型二分。**
Arroyo 每个 worker 构建全量物理图并实例化所有算子（`engine.rs:238-288`），远程任务的端点被本机"幽灵"中继接管（`engine.rs:599-665`）。ArkFlow 保持 per-node assignment 只编译本地子图，在 `graph.rs` 增加两类端点：上游侧 `RemoteWriter`（对应现有 `Forward/Broadcast` 旁新增 `Remote(Vec<QuadAddr>)`）、下游侧每条远程入边一个本地 flume channel，由 `NetworkManager` 的入站 pump 喂入——chain event loop 从 `inputs: Vec<Receiver<Envelope>>` 消费，对远程入边无感。这避免在每个 Agent 上构造/销毁全图算子，端点生命周期严格随 assignment。

**路由规则（必须区分帧类型）**：`Data` 按行级 key-group hash 选**单个**目标 quad（复用 `job.rs` 的 `key_group_for_key`/range）；`Barrier`/`Watermark`/`Eos` **广播到该边全部 quads**（每 quad 一条连接、各发一份，与 Arroyo signal 的逐 quad 发送一致）。若控制元素按 key 路由，未被选中的下游副本永远收不到 barrier，`Aligner` 死等，checkpoint 卡死——这是硬性规则，不是实现自由度。

**D3：背压 = 有界队列 + TCP 背压，无 credit 协议。**
入站 pump 在 flume `send` 上 await（通道满即停读 socket → TCP 窗口收缩 → 发送端 `write` 阻塞 → 上游 chain 的 `send_downstream` 阻塞），整链有界。出站侧每 quad 一个有界发送队列，写循环 100ms 定时 flush（Arroyo `network_manager.rs:267` 同款，可配置）。曾考虑 credit-based 流控，因 Envelope FIFO + barrier 搭车模型下有界队列已满足"never buffer unboundedly"而撤回。

**D4：Ack 三态镜像回执（本设计唯一的原创协议部件），上游聚合复用 `fanout_ack` 分支。**
Arroyo 无 Ack 概念（其源偏移随 barrier seal 批量提交）；ArkFlow 是逐批连续 ack。方案分两端：

*上游侧——writer 是 `fanout_ack` 的一个分支，不引入新聚合器。* 现有 `fanout_ack(parent, branches)` 的 `FanoutAckPart`（`input/mod.rs:96-170`）已实现分支计数、防重复、per-branch held 跟踪、abort/undo 补偿的全部语义。RemoteWriter 对上游 chain 呈现为普通本地下游分支：上游在既有交接点（`process_with_ack`/`fanout_ack(ack, outputs)`，`task.rs:2151-2194`、`2387-2416`）把分支 ack 交给 writer；writer 收齐该 quad **所有下游副本**的 `Acked{seq}` 后调用该分支的 `ack()`，`Held/Released` 回执转发为分支的 `mark_held()`/`release_held()`，出站队列满或发送失败时对分支 `abort()`（对应本地 send 失败路径）。补偿语义自动继承。代价：`FanoutAckPart` 需 pub 化或提供构造辅助。

*下游侧——`RemoteAck` 与同步 `mark_held` 的 outbox。* `Ack` trait 的 `mark_held`/`release_held` 是**同步** `fn`（`input/mod.rs:80-87`，调用点在 `Aligner::observe` 的同步上下文），而回发 `Held{seq}` 需要异步 IO。`RemoteAck` 内部持一个**仅控制帧的 unbounded outbox**：同步 `try_send` 投递、由出站写循环异步消费发送。控制帧量小（每批次至多 3 帧），不触碰数据缓冲有界的红线（Data 帧仍走有界路径）。降级路径：Held 帧丢失 → 上游该 ack 仍留在 drain 等待集合 → drain 超时 fail-closed——安全但降效，非数据丢失。`ack()` 是 async fn，直接 await 发送 `Acked{seq}`，无此约束。

*崩溃与失败语义。* 下游死/回执丢失 → 上游分支永不 ack → drain 超时 fail 本轮 → generation 重放置 → 从 sealed cut 重放（at-least-once）。**下游处理失败（非断链）在 v1 没有主动回执**：失败走下游本地 error 上报，上游分支靠 drain 超时发现（延迟一个 `BARRIER_DRAIN_TIMEOUT`）——fail-closed 成立，接受该延迟；`Failed{seq}` 主动回执列为 open question。源链 seal 代码（`task.rs:492-540`）零改动——未回执的 pre-cut 输出天然留在 cut 之外。

**D5：传输抽象 = `EdgeTransport` trait + TcpTransport/LoopbackTransport。**
`serve(addr)`/`connect(addr, quad)`/关闭与错误传播两实现，语义测试全部跑 Loopback（同进程双端、可注入断链/延迟），TcpTransport 仅测编解码与连通性。Agent（`agent.rs`）在 job kernel 启动时启动 listener，数据端口随注册/心跳上报 Hub，供 v2 放置寻址（本 change 仅上报，不消费）。

**D6：旁路边 v1 不过线。** error 边与 late-event route 边是旁路语义（不参与 barrier 对齐的普通数据路径），placement 校验继续强制其两端同节点；线协议本身不区分边类型，v2 放开仅是放宽校验。

## Risks / Trade-offs

- [对齐缓冲上限 × 网络 RTT：远程入边使 `Aligner.max_buffered` 更易触顶，checkpoint 轮次失败率上升] → 缓冲上限按入边数量缩放并纳入配置；对齐超时路径已有 fail-closed 语义，失败安全。
- [小批次高频场景帧开销：每 batch 一个 IPC 帧无跨批聚合] → 沿用 100ms flush 批量化写；吞吐不足时后续在 quad 队列内做微批聚合（须保持 barrier 在聚合中的位置，本 change 不做）。
- [中继缺失的代价：与 Arroyo 不同，远程端点必须严格随 assignment 生成/销毁，泄漏会悬持 socket] → 端点生命周期挂在 `KernelJobHandle` cancel 路径统一清理，测试覆盖 kernel 取消后的连接关闭。
- [回执与 fan-out 交错：`Acked` 到达顺序与发送序不同（各副本独立完成），`Held/Released` 与 `Acked` 可能乱序] → per-quad 按到达序驱动 `FanoutAckPart`（分支自身防重复、幂等）；乱序语义继承现有状态机并纳入 loopback 测试。
- [控制帧 outbox 丢失（`mark_held` 同步投递无法阻塞重试）] → 降级为 drain 超时 fail-closed（安全降效）；出站写循环消费 outbox 失败视为连接失败，走 fail-closed 路径。
- [每 quad 一条 TCP 连接数 = O(上游 subtask × 下游 subtask)] → v1 规模（个位数 Agent、有限并行度）可接受；subtask 数大时引入 Arroyo `InQReader` 式公平复用（后续 change）。

## Migration Plan

纯增量：默认 `shuffle` 关闭时行为与现状逐位一致（`assignments_for_nodes` 逻辑不动，`graph.rs` 走既有本地端点路径）。启用路径仅测试使用。回滚 = 不启用。无配置格式破坏。

## As-Built Notes（实现校准，2026-09-15）

实现过程中发现了几处比设计更优或必须修正的细节，均已回写：

- **D2 的最终形态比设计更干净**：没有新增 `EdgeTarget` 变体。`RemoteEdge.sender` 本身就是 `flume::Sender<Envelope>`，直接进入现有 `Forward/Broadcast/Partitioned` 通道向量；fan-out 分支 ack 在既有 `send_to_targets` 交接点交给 writer（泵）。`task.rs` 的分发与 abort 路径零改动。
- **D4 无需 pub 化 `FanoutAckPart`**：泵经 `Arc<dyn Ack>` trait 持有分支，`PendingReceipts` 是按 quad 的**回执路由器**（转发到分支），不是第二套聚合器——聚合、防重、补偿全部留在现有分支实现里。
- **seq 搭载在 `WireDataMeta`**（数据帧 JSON 元数据）而非帧头——帧头保持 24 字节定长。
- **新增 `Chain.edge_failures` + `run_graph_inner` watcher**：空闲链（阻塞在 input 上）永远不会在自身 send 路径上发现死边，manager 的失败通道经 watcher 取消链并将错误作为链返回值——填补了"下游处理失败/断链靠超时发现"在空闲链场景下的缺口。
- **新增 `REGISTRATION_GRACE`（10s）**：入站连接等待对端 quad 的本地通道注册完成，吸收两 Agent 独立建图的启动竞态。
- **`open_edge_deferred`**：图构建保持同步，连接在后台任务建立（含重试），边通道在连接期间照常收数据并施加背压；连接失败 → pending abort + 注册表条目清除 + 通道断开 = fail-closed。
- **对端中途死亡（CR 轮修复）**：入站连接按 quad 跟踪 Eos；连接结束而无 Eos ⇒ 向 manager 失败通道注入错误，下游链经 watcher 判失败——否则下游把崩溃当干净完结，违反 fail-closed 场景。回归测试 `peer_death_without_eos_fails_the_downstream_side`。
- **失败通道按 Agent 进程共享**：同机多 Job 同时启用 shuffle 时，一个 Job 的边失败可能被另一个 Job 的 watcher 消费（保守方向：多失败不丢数据）。Phase 3 需按 job/generation 定界。
- **flush 间隔 100ms 为常量**：tasks.md 标注"可配置"，v1 实现为常量（低价值配置），随真实延迟调优需求再开。
- **停止顺序与排空（verify 轮发现）**：`shutdown_interior_chain` 对输入通道的排空等待通道关闭，而远端 inbound 通道的 sender 由 manager 注册表与 serve_stream 缓存持有——只有对端 Eos（正常停止：source 链停止时向远端转发 Eos）或连接死亡（崩溃）才关闭。若 B 先停而 A 的连接仍活，B 的排空悬挂直到 A 停止（有 `KERNEL_TEARDOWN_JOIN_TIMEOUT` 上界兜底，且 Hub 的 job_stop 双节点下发使窗口很短）。测试 `remote_barriers_align...` 以 Eos 收尾固定了生产形状。
- **`operator_routing_index` 抽为共享函数**（graph.rs pub fn）：spec 的 `operators` 列表可含 source/sink 条目，quad 路由号非显而易见的 0/1；测试与 build 必须同源推导，手写索引会静默错配（本次 verify 轮实测踩中）。
- **Aligner 对齐缓冲**：`1024.max(512 × 入边数)`（`task.rs`），配置面留待后续。

## Open Questions

- 数据端口配置项的归属（Agent config 独立段 vs 复用现有 bind 配置）——tasks 中以独立 `data_port` 项落地，实现时可再收敛。
- Receipt 帧是否需要批量捎带（多个 seq 一帧）——v1 单 seq，度量后再决定。
- `Failed{seq}` 主动失败回执：v1 下游处理失败靠上游 drain 超时发现（延迟一个 `BARRIER_DRAIN_TIMEOUT`）；若实测失败传播太慢，增加第四种回执让上游分支立即 abort。
