# Proposal: fix-batch-processor-ack-contract

## Why

`batch` 处理器违反引擎自己的确认契约，且合并路径有两类静默错误：

1. **缓冲期间提前 ack**：`crates/arkflow-plugin/src/processor/batch.rs:105-119` 缓冲未满时返回 `Ok(ProcessResult::None)`，而内核对 None 的处理是立即结算 ack（`crates/arkflow-core/src/executor/task.rs:2710-2724`）——源位点/WAL 游标在行仍驻留内存缓冲（`batch.rs:41` 的 `Arc<RwLock<Vec<MessageBatchRef>>>`，从不持久化到状态后端）时就已提交。硬崩溃（SIGKILL/节点丢失）即丢失窗口内最多 `count` 批或整个 `timeout_ms` 周期的消息，违背引擎声明的 at-least-once。内核注释（task.rs:2720-2722）明确指出正确做法："Stateful buffering uses `Deferred` below to retain the acknowledgement until a later output is committed"——窗口算子已按此实现，batch 处理器没有。
2. **位置式合并**：`batch.rs:80-83` 用**首批的 schema** 调 `concat_batches`。Arrow 58 的该 kernel 完全不比较 schema、按位置取列（vendored `concat.rs:530-558`）：同类型不同键序的 JSON 行（`{"a":1,"b":2}` 与 `{"b":5,"a":6}`）合并后第二行 a/b **值互换**；第二条多一个字段则列越界 panic。仓库已有的正确工具 `normalize_and_concat`（`crates/arkflow-plugin/src/component/batch_merge.rs:31`，类型冲突报错、缺列 null 填充）就在旁边未被使用。
3. **count 语义偏差**：`batch.rs:58` 的 `batch.len() >= count` 数的是**批数**（`Vec<MessageBatchRef>` 长度）而非消息行数——上游多行批时 `count: 1000` 实际缓冲 1000 批，内存与触发延迟均偏离配置语义。

## What Changes

- batch 处理器改经 `process_with_ack`（`processor/mod.rs:82`）持有投递 ack：缓冲期间返回 `ProcessResult::Deferred`；flush（满批/超时/EOS/空闲 tick）发射合并批时经 `SingleWithAck` 携带组合 ack，下游写出成功后一并结算全部暂存 ack，失败按组合 ack 语义补偿。
- 合并路径改用 `normalize_and_concat`：异构 schema 归一合并（并集 + 缺列 null 填充），类型冲突显式报错——替换位置式 `concat_batches`。
- `count` 语义对齐为**消息行数**（与配置名 "Batch size" 的自然语义一致），元数据与文档明示。
- 处理器失败/关闭路径对暂存 ack 执行 abort（恢复后重放）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `message-acknowledgment`: ADDED requirement——缓冲型处理器投递 SHALL 延迟结算 ack 至 flush 发射被下游确认，禁止在数据仍驻留易失缓冲时提交源位点。
- `buffering-stage-drain`: ADDED requirement——batch 处理器合并 SHALL 经 schema 归一（并集/缺列 null 填充/类型冲突报错），禁止位置式拼接。

## Non-goals

- 不给 batch 缓冲引入持久化（Deferred-ack 后崩溃即重放，at-least-once 已足够；跨重启的去重属 L2/L3 范畴）。
- 不改 window buffer（tumbling/sliding/session）实现——它们走内核 journal 路径。
- 不引入 `count` 的批数/行数双模式配置（单一语义：行数）。
- 不处理 `buffer/memory.rs:127` 与 `processor/protobuf.rs:141` 的同类位置式 concat（protobuf 单描述符 schema 恒同、memory buffer 语义另议，留待后续审计）。

## Impact

- `crates/arkflow-plugin/src/processor/batch.rs`（process_with_ack、flush 合并、count 语义、失败路径）。
- `crates/arkflow-plugin/src/component/batch_merge.rs`（若需 pub(crate) 可见性微调）。
- 测试：batch.rs 内联 7 例扩充（Deferred 结算、合并归一、abort 路径、行数语义）；`crates/arkflow-core/src/executor/tests.rs` 若有 batch 处理器端到端用例同步。
- 文档：batch 处理器页（en/zh）说明 ack 延迟结算、schema 归一合并与 count 行数语义。
