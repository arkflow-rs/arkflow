# Proposal: add-stream-join-operator

## Why

双流 join 是流处理引擎最重的能力短板（PLANNING.md 第八节 P1 项）：此前 `OperatorKind::Join` 在 Job DAG 校验中被直接拒绝，唯一替代是 SQL processor 的批内临时表 join。内核地基（keyed 算子、多输入链、barrier 对齐、watermark 分发）均已就绪。

## What Changes

1. 新增统一内核 join 算子（`executor/join.rs`）：keyed interval join——左右两侧按 key 相等、事件时间差 ≤ `window_ms` 即匹配；inner join、即时发射（at-least-once）。
2. 输入侧别：join 所在链的两条入边按声明序定为左（input 0）/右（input 1）；链路循环为 join 链的每个批次追加 `__meta_input_index` UInt32 列。
3. 有界状态：每侧每 key 缓冲受 `max_per_key` 上限（最旧先逐出）；watermark 推进超过 `timestamp + window_ms + ttl_ms` 后逐出（`Processor::on_watermark` 钩子）。
4. 输出形态：`l_<原左列>` + `r_<原右列>` + `join_key`；要求每侧 schema 稳定（变更即报错）。
5. 状态恢复：无独立快照——恢复时源回退到已确认 checkpoint cut，缓冲由重放确定性重建。
6. Job DAG 放开 `OperatorKind::Join`：配置校验（left_key/right_key/window_ms/ttl_ms/max_per_key）+ 恰好两条入边的元数校验；join 链强制单线程（`processor_parallelism = 1`）。
7. Stream `join` buffer 报错文案更新为指向现在真实存在的 Job DAG join 算子。
8. 文档：distributed-jobs 的 join 边界章节改为「已支持 + 语义边界」；组件 schema 中 join 算子配置面。

## Capabilities

### New Capabilities

- `stream-join-operator`: 统一内核 keyed interval join 算子的行为契约（输入侧别、匹配语义、有界状态、输出形态、恢复语义）。

### Modified Capabilities

- `streaming-job-api`: Join 算子从「显式拒绝」改为「带配置与入边元数校验的支持」。
- `stream-config-compilation`: `join` buffer 拒绝文案指向现在存在的 Job DAG join 算子。

## Impact

- 新增 `crates/arkflow-core/src/executor/join.rs`（算子 + 7 个单测）。
- `crates/arkflow-core/src/executor/graph.rs`：Join 算子构造、`Chain.tags_input_index`、join 链单线程。
- `crates/arkflow-core/src/executor/task.rs`：`tag_input_index` 辅助 + 数据路径打标。
- `crates/arkflow-core/src/job.rs`：Join 校验放开 + 两入边校验。
- `crates/arkflow-core/src/executor/stream_compiler.rs`：报错文案更新。
- 端到端测试：双源 → join → sink 全链路（`executor/tests.rs`）。

## Non-goals

temporal join（维表 lookup）、非 equi-join、跨节点 shuffle join、join 状态独立快照（重放重建已覆盖）、outer join 语义、乱序 watermark 对齐的 per-side 独立 watermark（复用链级最小 watermark）。
