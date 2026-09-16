## 1. 线协议（remote.rs 基础）

- [x] 1.1 定义 `Quad`（src_op/src_subtask/dst_op/dst_subtask）、`FrameKind`（Data/Signal/Receipt）、`FrameHeader` 及其编解码（参考 Arroyo `Header::from_bytes`/`write`，长度上限校验）
- [x] 1.2 实现 Data 帧负载的 Arrow IPC 编解码（`IpcDataGenerator::encoded_batch` + per-quad `DictionaryTracker`；接收端 `read_message` 还原，含字典列），单测：字典编码列往返等值
- [x] 1.3 实现 Signal 帧负载 serde 编解码（Barrier/Watermark/Eos）与 Receipt 帧负载（`Acked/Held/Released + seq`），单测：roundtrip
- [x] 1.4 `Envelope` 线上映射辅助：`Envelope + seq` ↔ 帧的转换（Data 拆出 ack 由发送端持有；receipt 不承载 batch），单测：非法帧头/超长帧拒绝

## 2. 传输抽象与 NetworkManager

- [x] 2.1 定义 `EdgeTransport` trait（serve/connect/关闭/错误传播），实现 `LoopbackTransport`（同进程双端，可注入断链与延迟，测试用）
- [x] 2.2 实现 `TcpTransport`：listener、每 quad 出站连接、指数退避重连（参考 Arroyo `OutNetworkLink::connect`）、入站会话分发（`Quad` → 本地 channel）
- [x] 2.3 出站写循环：每 quad 有界发送队列 + 100ms 定时 flush（可配置），入站 pump：socket 读 → flume send（满则停读，验证反压传播路径存在）
- [x] 2.4 集成测试：TcpTransport 双端 loopback 端口上跑通 Data/Signal/Receipt 三种帧、断连后错误传播到两端、关闭后 socket 无残留（`ss`/计数断言）

## 3. 内核远程端点接线

- [x] 3.1 `graph.rs`：`EdgeEndpoint` 增加 `Remote(Vec<QuadAddr>)` 出边变体与远程入边注册（喂入 `inputs: Vec<Receiver<Envelope>>` 的既有消费路径），shuffle 关闭时保留现行拆边报错（`graph.rs:462`）为条件分支
- [x] 3.2 `RemoteWriter` 作为 fan-out 分支接入：Data 按行级 key-group 路由（复用 `job.rs` 的 `key_group_for_key`/range）选单目标 quad；Barrier/Watermark/Eos 广播全部 quads（硬性规则，漏发即对齐死锁）；交接点挂现有 `process_with_ack`/`fanout_ack(ack, outputs)` 调用处（`task.rs:2151-2194`、`2387-2416`）
- [x] 3.3 pub 化 `FanoutAckPart`（或提供 `fanout_ack` 分支构造辅助）：writer 持有一个分支 ack，出站队列满/发送失败时对分支 `abort()`（对齐本地 send 失败路径）
- [x] 3.4 `RemoteAck`（下游侧）：`ack()`（async）直接 await 发送 `Acked{seq}`；`mark_held`/`release_held`（同步 fn，来自 `Aligner` 同步上下文）经**仅控制帧的 unbounded outbox** 同步 `try_send` 投递、出站写循环异步消费；单测：Held 丢失时上游留在 drain 集合、超时 fail-closed
- [x] 3.5 上游回执消费：per-quad 收 `Acked/Held/Released`（允许乱序、防重复），`Acked` 齐备 → 分支 `ack()`；`Held/Released` → 分支 `mark_held()/release_held()`——全部转发给现有 `FanoutAckPart`，不引入第二套聚合器；单测覆盖乱序/重复组合
- [x] 3.6 `Aligner.max_buffered` 按入边数量（含远程入边）缩放并纳入配置，防网络 RTT 放大触顶
- [x] 3.7 loopback 全语义测试：跨端点 FIFO（barrier 相对数据位置）、控制元素广播全副本、双入边（本地+远程）Aligner 对齐、慢消费者反压（writer 发送 await 挂起）、断链 fail-closed（两端链路错误、attempt 失败路径触发）、下游处理失败靠 drain 超时发现

## 4. Agent 接入与配置

- [x] 4.1 shuffle 配置项（默认关闭）+ Agent 数据面 `data_port` 监听生命周期：随 kernel 启动/取消开关，listener 与出站连接统一在 `KernelJobHandle` 取消路径清理
- [x] 4.2 Agent 注册/心跳 capabilities 增加 `network_shuffle` 与数据端口上报（Hub 侧仅存储展示，不消费——放置放开属后续 change）
- [x] 4.3 默认关闭下的回归验证：`cargo test --workspace --all-targets` 全绿 + 一个现有分布式 Job 的 e2e 行为不变（无新端口监听断言）

## 5. 收尾

- [x] 5.1 `cargo clippy --workspace --all-targets` 清零；若注册/配置面有暴露，按 AGENTS.md 重新生成 `docs_inventory_snapshot`
- [x] 5.2 openspec 校验通过（`openspec validate add-network-shuffle-data-plane`），对照 spec 逐条勾验场景覆盖
