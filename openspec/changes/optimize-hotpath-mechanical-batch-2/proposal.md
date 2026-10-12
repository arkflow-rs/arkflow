## Why

热路径审计第二批候选（`openspec/PLANNING.md` 第 10.2 节）经 2026-10-11 逐项代码核实：11 项中 9 项完全成立、2 项需修正。这些热点分布在 codec、input/output、buffer 与执行内核的每批/每行路径上：protobuf encode 每 cell 付一次名字哈希 + Arc bump（`crates/arkflow-plugin/src/component/protobuf.rs:655`），Kafka output 每 payload 冗余深拷贝且每条消息抢一次 tokio Mutex（`crates/arkflow-plugin/src/output/kafka.rs:278-286,330-334`），stdout 经 `LineWriter` 每行一次 syscall（`crates/arkflow-plugin/src/output/stdout.rs:69-87`），generate input 每条 clone 同一模板且 benchmark 前端失真（`crates/arkflow-plugin/src/input/generate.rs:82-93`），memory buffer 每次写持写锁 O(n) 求和（`crates/arkflow-plugin/src/buffer/memory.rs:162-184`），partitioned 路由 O(subtasks×rows) 全批掩码+拷贝（`crates/arkflow-core/src/executor/task.rs:3093-3138`），fanout_ack 每分支 3 次分配（`crates/arkflow-core/src/input/mod.rs:96-142`），TrackingAck 每源批一个 tokio Mutex（`crates/arkflow-core/src/executor/commit.rs:433`），窗口算子每行 3+ 次 String clone/双查找且每批全量克隆打开窗口（`crates/arkflow-core/src/executor/window/operator.rs:836-899,94-101`），event_time_gate 每行 3-6 个堆 Vec、物理分区切分每行 `format!`/`to_owned`（`crates/arkflow-core/src/executor/event_time_gate.rs:968-1160,1331-1407`）。

延续第一批 `optimize-hotpath-mechanical-batch`（2026-10-07，3 项机械修复打包）的先例，本 change 打包第二批中**语义保持、无需新设计**的机械项：每项独立可回退，既有等价/差分/并发测试全绿为门槛，release 计时测试量化收益。

## What Changes

- **protobuf encode 列式预解析**：`arrow_to_protobuf`（`component/protobuf.rs:627-788`）把已在 644 行解析却被丢弃的每列 `FieldDescriptor` 提升到行循环外，逐 cell 改 `set_field_by_name` → `set_field(&fd, v)`，消除每 cell 名字哈希 + HashMap 查找 + Arc bump；输出 `Vec<Bytes>` 与改造前字节级一致。新增 `#[ignore]` 计时测试（对齐 `protobuf_batch_decode_timing` 模式）。
- **Kafka output**：删除 codec 路径的冗余 `.map(|p| p.to_vec())`（`output/kafka.rs:283,575`；`arkflow_core::Bytes` 本就是 `Vec<u8>`）；`send_futures` 的 tokio Mutex 改 std Mutex（`lock()` 段内无 await 点）；flush/close 的串行 `future.await` 改有界并发观察回执，保持首错上报与 ack 语义不变。
- **stdout 缓冲**：writer 包 `BufWriter`，`writeln!` 逐行触发 `LineWriter` flush 的每行 syscall 变整批一次落盘；UTF-8 有效时避免 `from_utf8_lossy` 分配。
- **generate input 模板复用**：模板 bytes 预计算一次；配置 codec 时解码一次后按行复制扩到 batch_size（行值/元数据与逐条解码完全一致），消除每条 clone + 重复 JSON 解码——同时修复 benchmark 套件（`benchmark.rs:72-105` generate 前端）的度量失真。
- **memory buffer 增量计数**：`write` 持写锁 O(n) 全队列求和（`buffer/memory.rs:173`）改为 push/drain/clear 时维护的增量计数，O(1) 容量判断。
- **partitioned 路由单趟化**：`partition_batch_by_key_hash`（`executor/task.rs:3093-3138`）由每子任务全批掩码 + `filter_record_batch` 拷贝改为单趟按哈希分组行号、每非空子任务一次 gather；null 键与 key_group_range 归属语义逐字节不变。
- **fanout_ack 分配收敛**：每分支 2 个 `Arc<AtomicBool>` + 1 个 `Arc<FanoutAckPart>`（`input/mod.rs:109-118`，≈3n+3）改为共享状态内 `Box<[BranchFlags]>` 按位/按槽打包，子 ack 携 `(Arc<state>, index)`，降到 ≈n+3；hold/ack/abort/重试语义不变。
- **TrackingAck 去异步锁**：`ack_lock: tokio::sync::Mutex<()>`（`executor/commit.rs:433,445`）改 std Mutex，`inner.ack().await` 移出 guard 外执行；每源批 2 Arc + 1 异步 Mutex 分配中消灭后者，tracker 计数（dispatched/acked/aborted/undone）不变。
- **窗口算子**：状态键 `String` → `Arc<str>`（clone 从堆分配变引用计数）；accumulate 每 membership 3 次 key clone + 双 BTreeMap 查找（`window/operator.rs:865-899`）改 entry 单趟；`runtime_snapshot` 每批全量 `buffers.clone()`（`operator.rs:94-101`，process_internal 每批调用 1508）改为仅对本次 accumulate **touched 窗口**取 before-image，错误回滚/ack 补偿语义与全量快照逐字节等价。
- **event_time_gate**：`classify_row` 每行 3-6 个堆 Vec（`event_time_gate.rs:995-1159`）改 scratch 复用/内联判定，决策结果不变；`split_by_physical_partition_for_source` 每行 `to_owned` + `format!("__arkflow_source__:{id}")`（`event_time_gate.rs:1345`、`event_time.rs:46-58`）改每批 distinct 分区仅构造一次。

**审计修正（只修正认知，不改代码）**：① Kafka 事务路径为每 `write_batch` 1-2 次 broker 往返而非审计原述"每条两次"（`output/kafka.rs:426-561`：begin 为本地操作、send 为异步入队、commit 为每批一次阻塞往返）——无需立项；② `collect_outcomes` 全 Emit 批在 arrow-select 58.4 已走 `IterationStrategy::All` 零拷贝切片，审计原述"全批深拷贝"过时，仅混合结果组仍按组拷贝——本 change 不动。

## Capabilities

### New Capabilities

（无——本 change 全部为既有行为的性能保持性改造）

### Modified Capabilities

- `protobuf-codec`: encode 路径新增"每列 FieldDescriptor 预解析 + 逐 cell `set_field`"要求与字节级等价场景（对称于已归档的 decode 列式要求）。
- `columnar-window-operators`: 键类型改为 `Arc<str>`、entry 单趟累积的语义保持场景；快照从"每批全量克隆"改为"touched 窗口 before-image"，回滚等价性与分配不随打开窗口数增长成为钉死不变量。
- `event-time-processing`: gate 行决策分配不变量（修改既有分类一致性 requirement）+ 新增物理分区切分"每 distinct 分区构造一次身份、行归属等价"requirement。

不设 delta 的项：partitioned 路由单趟化（row→子任务归属语义已由 `task.rs` routing_tests 钉住，行为不变，无 spec 级变化）；fanout_ack 与 TrackingAck 分配收敛（`message-acknowledgment` spec 已钉住 ack/undo/abort 语义，本次只改分配结构）；Kafka output、stdout、generate input、memory buffer 均为纯实现级。

## Impact

- **插件 crate**：`crates/arkflow-plugin/src/component/protobuf.rs`（encode）、`output/kafka.rs`、`output/stdout.rs`、`input/generate.rs`、`buffer/memory.rs`。
- **内核 crate**：`crates/arkflow-core/src/executor/task.rs`（partitioned 路由、TrackingAck 构造点）、`executor/commit.rs`、`executor/event_time_gate.rs`、`executor/window/operator.rs`（含 `Arc<str>` 键类型波及 `window/` 内签名）、`input/mod.rs`（fanout_ack）。
- **测试/基准面**：新增 `protobuf_batch_encode_timing`（`#[ignore]` release 计时，对齐 decode 侧模式）；window `tests.rs`（≈4.6k 行）、`task.rs` routing_tests、`commit.rs` ack 测试、`memory.rs` 并发测试、generate/stdout/kafka 既有单测全绿为门槛；`tiny_suite_completes_with_positive_throughput` 场景数断言不受影响（不新增 benchmark 场景）。
- **无依赖变更、无配置格式变更、无 YAML/文档语义变更**；docs 面仅实现细节变化，不触发生成物。

## Non-goals

- **SQL processor 物理计划缓存**：需 SwapBatchTable 自定义 ExecutionPlan 设计，独立 change。
- **debezium 列式累积**（每消息 3 次 JSON 遍历 + before 深拷贝）：终态形态是列式累积进 builder，独立 change。
- **VRL 合并单批输出**（每行独立 MessageBatch + 每格 key String）：独立 change。
- **SQL output 列式转换**（逐 cell async + 三重深拷贝）：独立 change。
- **Kafka 事务路径改造**：审计原述"每条两次往返"经核实不成立（每批 1-2 次），不立项。
- **event_time_gate `collect_outcomes` 混合结果组拷贝**：全 Emit 快路径已被 arrow 零拷贝切片覆盖，混合组优化收益不明确，观察本 change 后的 profile 再定。
- **Avro L2 reader 复用**：上游 apache-avro API 阻塞（owned-reader 能力），维持 backlog。
