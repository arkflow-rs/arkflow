## Why

`optimize-avro-columnar-decode`（PR #1310）与 `optimize-protobuf-batch-decode`（PR #1312）把 schema_registry 解码路径的每消息成本压到列式累积后，2026-10-07 的四路热路径审计（codec / 执行内核 / processor / IO，结论留档 PLANNING 第十节）发现同类成本仍在其他入口与内核路径上重复支付：

- **独立 protobuf codec/processor 未接入现成的列式转换器**：`codec/protobuf.rs:120-157` 与 `processor/protobuf.rs:128-142` 仍逐消息调 `protobuf_to_arrow`（每消息重建全部 `Field`/`Schema`/单值数组）再 `normalize_and_concat` 全量拷贝；同一优化（`ProtobufBatchConverter`）已在 schema_registry 路径实测 ×18.5，这是唯一没接上的入口，且该入口所有消息共享同一 descriptor，连并集合并都不需要。
- **窗口算子行循环内整列 cast（O(n²)）**：`executor/window/operator.rs:862-868`，value 列为 Int8/16/32 或 UInt* 时每行把**整列**重新 cast 成 Int64 并分配新数组——1000 行批 = 1000 次整列 cast，属退化 bug 而非单纯低效。
- **每批指标查找的锁 + 分配**：`executor/metrics.rs:76-83` `KernelMetrics::chain()` 每次调用做 std Mutex 加锁 + `task_id.to_owned()` String 分配 + BTreeMap 字符串查找，`task.rs:2610/2669` 每批必调 2 次（dispatch + process），多链并发时共享同一把 kernel metrics 锁互相竞争。

三项均为机械修复、无行为语义变化；审计中的第四候选（SQL `target_partitions=1`）经实施后实测 GROUP BY 回归 ~11% 而**主动剔除**——多分区给聚合提供了查询内跨核并行，收益超过 spawn/concat 开销（详见 design.md D3 的实测记录）。

## What Changes

- `codec/protobuf.rs` decode 循环改用 `ProtobufBatchConverter`（push/finish 直出单批），删除逐消息 `protobuf_to_arrow` + `normalize_and_concat` 路径；skip 模式下坏消息不 push、逐消息错误语义与文案不变。
- `processor/protobuf.rs` decode 同样接入转换器，processor 行为语义不变。
- `executor/window/operator.rs` accumulate 的 value 列窄整型 cast 提到行循环外，每列每批一次（`BatchValueColumn`）。
- `executor` 的 `ChainHooks` 增加 `chain_metrics: Option<Arc<ChainMetrics>>` 构建期解析一次；`dispatch_data`/`process_chain` 改用预解析引用，消除每批 Mutex + String + BTreeMap 查找；`KernelMetrics::chain()` 保留（构建期注册与快照路径不变）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `protobuf-codec`: 「Decoded messages share a stable schema」requirement 增加 codec/processor 入口的列式累积表述——共享 descriptor 的批 SHALL 经列式转换器产出一个多行批次，schema 语义（全字段集、全 nullable）不变。
- `columnar-window-operators`: 「Keyed window aggregation state」requirement 增加值列规范化复杂度约束——窄整型 value 列的 Int64 规范化 SHALL 每批至多一次，不得在行循环内重复整列 cast。

## Impact

- `crates/arkflow-plugin/src/codec/protobuf.rs`、`crates/arkflow-plugin/src/processor/protobuf.rs`：decode 主循环重写为转换器调用（错误语义/文案逐字保留）；`component/protobuf.rs` 的 `protobuf_to_arrow` 降为 `#[cfg(test)]` 等价性对拍 oracle。
- `crates/arkflow-core/src/executor/window/operator.rs`：cast 位置移动，无 schema/状态格式变化。
- `crates/arkflow-core/src/executor/{task.rs, kernel_handle.rs}`：ChainHooks 接线（worker pool 一并传递）。
- 无公开 API 破坏、无依赖变化、无配置变化。
- 性能验证：既有 `protobuf_batch_decode_timing` 基准覆盖 ①；benchmark 套件前后对比 + 窗口大批线性度守卫测试覆盖 ②③。

## Non-goals

- 不做 SQL processor `target_partitions` 调整（已实测回归并剔除，见 Why；未来若重估须以物理计划分析立项）。
- 不做 SQL processor 物理计划缓存（SwapBatchTable 需自定义 ExecutionPlan，独立立项）。
- 不动 protobuf encode 方向（逐 cell 按名查找，另行批量）。
- 不动 JSON 双遍解析/schema 缓存、Kafka input 逐条出批、event_time_gate 行级分配、窗口状态快照——审计已定位（PLANNING 第十节），各自独立 change。
- 不改变任何 codec 的错误语义、skip/fail 行为、元数据列或输出 schema。
