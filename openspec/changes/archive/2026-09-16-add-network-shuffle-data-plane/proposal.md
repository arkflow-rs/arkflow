## Why

ArkFlow 的分布式运行时目前没有任何跨节点数据通道：执行内核的所有边都是进程内 flume 通道（`crates/arkflow-core/src/executor/graph.rs:20-33` 的 `Sender<Envelope>`/`Receiver<Envelope>`），放置算法因此按连通分量整体落位（`crates/arkflow-core/src/job.rs:706-712`，注释明言 "The current runner has no cross-node shuffle/transport"），且图构建对拆边硬性报错（`graph.rs:462` "connected tasks must be co-located"）。结果是"分布式"只体现在 Job 级分发：单算子无法横向扩容到多节点，集群吞吐受限于单进程。

本 change 引入跨节点网络 shuffle 数据面（参考 Arroyo `network_manager.rs` 的成熟设计），使算子边可以跨 Agent 传输，为后续按 key-group 拆分放置（Phase 4）铺平数据面基础。

## What Changes

- 新增 `Envelope` 线协议：裸 TCP + 固定帧头（src/dst 四元组、长度、类型）+ Arrow IPC 数据负载 + serde 控制负载；`Data`/`Barrier`/`Watermark`/`Eos` 同通道严格 FIFO 交错，下游 barrier 对齐逻辑（`Aligner`）零改动。
- 新增执行内核远程边端点：`graph.rs` 支持 `RemoteWrite`（上游侧，含分区路由）与 `RemoteRead`（下游侧，喂本地 channel）两类端点；placement 标记跨节点的边在启用 shuffle 时走远程端点，硬报错仅保留在禁用时。
- 新增 `NetworkManager`（`arkflow-core/src/executor/remote.rs`）：TCP listener、每 quad 一条出连接、指数退避重连、100ms 批量 flush；loopback transport 用于全语义离线测试。
- 新增 Ack 三态镜像回执协议（ArkFlow 独有，Arroyo 无对应）：反向帧 `Acked{seq}`/`Held{seq}`/`Released{seq}` 镜射下游 `Ack` 生命周期（`ack`/`mark_held`/`release_held`），上游 edge writer 收齐所有下游副本回执后才 complete 复合 ack——保持 `input-durability` 的"逐批连续 ack"契约跨节点成立。
- 背压遵循 Arroyo 模式：有界本地队列（沿用 1024）+ TCP 自身背压 + 公平写复用，不引入 credit 协议。

**Non-goals**（本 change 明确不做）：

- 不做放置算法放开（`assignments_for_nodes` 仍按连通分量落位）——数据面就绪后由后续 Phase 4 change 处理。
- 不做跨节点 checkpoint 协调改造（barrier 可过线，但完成判定仍为 per-kernel）——Phase 3 change 处理。
- 不做动态 rescale、非对齐 checkpoint、断线重连续传（断链 = attempt 失败，走既有 generation 重新放置）。
- 不做数据面鉴权/mTLS（与 Arroyo 一致信任网络；控制面已有 node_token 体系不变）。
- 不做 Stream 引擎路径（`stream_compiler` 单机流）的网络化。

## Capabilities

### New Capabilities
- `network-shuffle-data-plane`：跨节点执行边传输——线协议与帧编解码、远程边端点接线、NetworkManager 连接管理、背压语义、Ack 三态镜像回执、失败与恢复语义（断链 fail attempt）。

### Modified Capabilities
<!-- 无：现有 spec 的 REQUIREMENT 均不因本 change 改变。graph.rs 的拆边报错仅在 shuffle 启用时放宽，
     属于新能力的条件分支而非既有需求的语义变更；checkpoint/durability 契约（input-durability、
     checkpoint-recovery）原样保持，回执协议正是为守住它们而设计。 -->

## Impact

- **代码**：`crates/arkflow-core/src/executor/`（新 `remote.rs`；`graph.rs` 端点分支；`envelope.rs` 序列化辅助；`task.rs` 远程入边的 Ack 包装）、`crates/arkflow-server/src/agent.rs`（数据面 listener 生命周期，capabilities 上报）。
- **依赖**：零新增外部 crate（tokio/arrow/serde 均已有）；帧编解码为手写，参考 Arroyo `network_manager.rs` 的 `Header`/`read_message`/`write_message_and_header`。
- **协议契约**：`input-durability`、`checkpoint-recovery` 的不变量必须原样保持——回执聚合推迟源 ack complete，对齐 hold 期间回执镜像 mark_held/release_held。
- **测试**：loopback transport 承载全部语义测试（FIFO、barrier 对齐过远程端点、回执聚合、断链 fail-closed）；不需要多进程测试即可覆盖 v1 语义。
