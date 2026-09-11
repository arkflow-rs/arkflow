# Rebuild: Unified Streaming Engine Kernel

## Why

ArkFlow 目前维护两套执行运行时，且新运行时的执行内核远未达到先进流引擎（Flink / RisingWave / Arroyo）的水准：

1. **YAML `Stream` 运行时**（`crates/arkflow-core/src/stream/mod.rs:39-56`）：线性管道、flume 通道 + 多 worker + 序号有序输出、WAL at-least-once。成熟但无状态恢复、无事件时间窗口、无 DAG。
2. **v1 Job 运行时**（`crates/arkflow-core/src/job_runner.rs:153-162`）：有 DAG/状态/checkpoint 契约，但执行内核是**同步递归 dispatch**——`job_runner.rs:849-878` 读一批 → 递归处理 → 直接写 sink → ack，**每个源同一时刻只处理一个 batch，无流水线并行、无反压通道、无算子间并发**。

具体硬伤（file:line 证据）：

- **无流水线**：`job_runner.rs:562-598` 主循环 `select` 所有 input，逐批 `dispatch_event_action` → `dispatch` 递归展开整条 DAG 后才读下一批。source、算子、sink 无法重叠执行，吞吐上限 = 单批全链路延迟的倒数。
- **快照停世界**：`job_runner.rs:523` `checkpoint_gate.write()` 拿全局写锁、`run_connected` 每批拿读锁——checkpoint 期间整个 Job 停止处理，不是 Flink 式异步 barrier 对齐。
- **有状态算子是计数器存根**：`job_runner.rs:42-151` `StatefulProcessor` 只做 keyed 计数加列；`state.rs:126-149` `WindowAccumulator` 存在但**无任何调用方**。真正的窗口聚合不存在。
- **逐行复制**：`job_runner.rs:767-778` `filter_row` 为每行重建整个 RecordBatch（O(n) 次 batch 复制），列式优势尽失。
- **迟到事件按行切分后窗口仍不存在**：event-time 路径只做 Hold/Emit/Route 决策（`event_time.rs:163-192`），"窗口聚合" 依赖下游 SQL processor 自行完成，无 keyed 状态窗口算子。
- **双运行时并存**：`config.rs:97-105` `EngineConfig` 只有 `streams` 字段；Job 只能经 Hub API 创建（`arkflow-server/src/agent.rs:300-466`）。本地 YAML 用户无法使用新能力，两套代码重复演进。

用户已明确授权破坏性更新，目标是对标市面上最先进的流处理产品。对标的验收标尺（参照 Flink 1.20 / RisingWave 2026 公开语义）：

| 能力 | 先进产品 | ArkFlow 现状 |
| --- | --- | --- |
| 流水线执行（算子并发重叠） | ✅ | ❌ 逐批递归 |
| 异步 barrier checkpoint（不停世界） | ✅ | ❌ 全局闸门 |
| 事件时间 keyed 窗口算子（列式） | ✅ | ❌ 无窗口算子 |
| 算子链融合（chain 内零通道开销） | ✅ | ❌ 无 |
| 反压（有界通道传播） | ✅ Stream 有 / Job 无 | 半 |
| Exactly-once（offset 入 checkpoint + sink 幂等/事务） | ✅ | 部分（EOS 仅 sink 侧） |
| 本地/分布式同一内核 | ✅ | ❌ 两套运行时 |

## What Changes

以**统一执行内核（Unified Execution Kernel）**替换两套运行时的执行层：

1. 新增 `executor` 模块：`ExecutionGraph`（JobPlan → 算子链融合的物理图）、有界通道 `Envelope` 边（Data/Barrier/Watermark）、每 vertex 独立 task 事件循环（流水线并行 + 通道反压）、算子链融合（Map/Filter 链零通道跳数）。
2. **异步 barrier checkpoint**：barrier 作为控制信封在数据流中按序流动，vertex 对齐输入后异步快照、立即放行数据，替换 stop-the-world 闸门；source 位置随 barrier 注入采集。
3. **列式窗口算子**：vectorized 窗口分配（Arrow compute 按 `window_start` 整批计算）+ keyed 聚合状态（Arrow IPC 序列化进 StateBackend），watermark 触发发射；同时支持 processing-time trigger 模式（兼容 Stream 攒批语义）。
4. **StreamConfig → JobSpec 编译器**：现有 YAML 零改动编译为 JobSpec（input→source、processors→Map 链、buffer window→窗口算子 processing-time 模式、error_output→错误边、durability→source WAL）。
5. **本地 Job 模式**：`EngineConfig` 新增 `jobs` 字段；单二进制本地运行 Job（内嵌 coordinator，无需 Hub）。
6. **退役 `Stream` 运行时**：`stream/mod.rs` 的执行器删除，`StreamConfig` 保留作为编译器输入（**BREAKING**：Stream 执行行为由统一内核承载；buffer 插件降级为兼容映射，不再独立执行）。
7. **分布式模式重接**：Agent 的 `JobRuntime` 改用统一内核（按本地子图构建），Hub API 不变。
8. 状态后端值支持列式（Arrow IPC）编码，keyed 窗口/聚合状态按 (key, window) 命名空间存储。

### Non-goals

- 不实现跨节点数据 shuffle/网络传输（通道仍为进程内；任务共置约束保留，跨节点 exchange 留待后续 change）。
- 不实现 SQL 物化视图、流式数据库查询接口。
- 不替换 Hub–Agent 控制面协议、Console 与 API 契约。
- 不新增连接器；插件层（Input/Output/Processor/Codec trait）保持不变。
- 不实现 two-phase commit sink（Kafka L3 `send_offsets_to_transaction` 仍为 future）。

## Capabilities

### New Capabilities

- `unified-execution-kernel`: 执行图、通道边、算子链融合、task 事件循环、反压传播与本地/分布式同一内核。
- `async-checkpoint-barriers`: barrier 信封流动、输入对齐、异步快照、source 位置采集与 checkpoint 完成语义。
- `columnar-window-operators`: 列式窗口分配、keyed 聚合状态、watermark/processing-time 双触发模式、迟到事件处理。
- `stream-config-compilation`: StreamConfig → JobSpec 编译规则、YAML 兼容性、error_output/WAL/buffer 语义映射。

### Modified Capabilities

- `distributed-job-runtime`: 统一内核替换 `SingleComputeJobRunner`；本地模式无需 Hub。
- `checkpoint-recovery`: 快照不再停世界；恢复路径经统一内核。
- `keyed-state-backend`: 值编码支持 Arrow IPC 列式；窗口/聚合状态命名空间约定。
- `event-time-processing`: 窗口触发与算子集成（替代 buffer 层窗口语义）。

## Impact

- **重写** `crates/arkflow-core/src/job_runner.rs` → 新 `crates/arkflow-core/src/executor/`（graph/channel/task/barrier/window/chain）。
- **删除** `crates/arkflow-core/src/stream/mod.rs` 执行器（保留 `StreamConfig` 类型，迁移至编译器模块）。
- **修改** `crates/arkflow-core/src/config.rs`（`jobs` 字段）、`engine/mod.rs`（本地 Job 执行）、`runtime.rs`（RuntimeEntry 承载 Job）。
- **修改** `crates/arkflow-server/src/agent.rs`（`JobRuntime` 用内核重建图）。
- **兼容** `arkflow-plugin` 全部插件经 `JobComponentAdapter` 复用（`job.rs:811-825`），零插件改动。
- **影响 spec**：`stream-backpressure`、`stream-runtime-control`、`input-durability` 的验收由统一内核承载，行为语义保持。
- 测试：内核单测（通道/链/barrier/窗口）、StreamConfig 编译黄金用例、examples 全量回归、故障注入（barrier 期间写入、恢复重放）。
