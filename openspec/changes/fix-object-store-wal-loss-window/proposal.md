# Proposal: fix-object-store-wal-loss-window

## Why

规格（`input-durability` "Segment-based batching with a bounded loss window"）允许 object-store WAL 的有界丢失窗口，但现行实现把**源位点已提交的记录**也留在了窗口内：`Wal::acknowledge`（`crates/arkflow-core/src/wal/mod.rs:478-572`）在 WAL 游标推进后立即执行源提交，而 object-store 后端的 `advance_cursor`（`crates/arkflow-plugin/src/wal/s3.rs:748-749`）只推进内存 `acked_hwm`——持久 manifest 游标被钳制在 `max_sealed_seq`（`s3.rs:1043`），段对象真正 PUT 要等 flusher 下一个 tick（默认 1–10s，`crates/arkflow-plugin/src/wal/config.rs:34-54`）。崩溃时序：读 → `Wal::flush`（仅内存暂存 `active.bytes`，`s3.rs:712`）→ 下游处理输出 → 源 offset 提交 → 崩溃 → 段对象从未存在、恢复 LIST 无可重放 → Kafka 从已提交 offset 继续——**已处理、已输出、已 ack 的记录永久丢失，无重放路径**。

这与本地 redb 后端不对称：redb 的 `flush_pending` 是持久 commit，`WalInput::read` 发布前的显式 flush（`crates/arkflow-core/src/executor/stream_adapter.rs:285-290`）在本地后端确实构成持久交接；同一份配置在 object-store 后端静默退化为秒级丢失窗口（`s3.rs:24` 注释明示弃用该契约）。另外 flusher 唤醒分支吞掉 flush 错误（`wal/mod.rs:376`）：持久故障下静默热重试，无计数、无 warn，"重试中"与"已损坏"不可区分。

## What Changes

- **源 ack 门控段密封**：object-store 后端上，`Wal::acknowledge` 在执行源提交前 SHALL 等待该条目序号进入已密封段（sealed segment object）。规格中的"丢失窗口"收窄为"重放窗口"——未密封条目在节点丢失后经源重投恢复（at-least-once），不再丢失。
- **flusher 失败可见性**：唤醒分支 flush 失败 SHALL 记入指标并以限速 warn 输出，连续失败升级为 error；关闭分支维持现有 `close()` 重新上抛语义不变。
- 规格措辞更新：`input-durability` 两个 requirement 从"未 flush 即丢失"改为"未密封条目由源重投重放；已完成的源 ack 意味着条目已密封"。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `input-durability`: MODIFIED "Object-store WAL survives node loss"（未 flush 条目从"丢失"改为"源重投重放"）与 "Segment-based batching with a bounded loss window"（源 ack 仅在条目密封后完成，窗口语义从丢失改为重放）；ADDED "WAL flusher 失败 SHALL 可观测"。

## Non-goals

- 不引入 per-entry 同步——规格已拒绝 object-store + `per_entry` 组合，维持不变。
- 不改段批参数与默认值（`max_entries`/`max_bytes`/`flush_interval`），不实现 D7 段回收（密封段 GC 另立提案）。
- 不触碰本地 redb 后端路径与 `wal-manifest-write-coordination` 的 ETag 协调协议。
- 不改 `PerEntry` 在本地后端的内联 fsync 行为。
- 不为旧语义提供逃逸开关（吞吐敏感部署应调 `flush_interval`，而非恢复丢失窗口）。

## Impact

- `crates/arkflow-core/src/wal/mod.rs`：`acknowledge` 的 object_store 分支增加密封等待（有界、取消安全）；flusher 唤醒分支错误可见性。
- `crates/arkflow-plugin/src/wal/s3.rs`：暴露已密封序号（`sealed_seq` 原子量）+ 密封完成通知原语；`seal_active_segment` 成功后发布。
- 测试：`wal/mod.rs` 内联测试、`stream_adapter.rs` 测试、`s3.rs` 测试（51 例）新增回归；`wal_optimization_e2e.rs` 吞吐断言复核。
- 文档：`docs/docs/` 与 zh-Hans 树的 durability/WAL 页面需说明源 ack 门控语义与 ack 延迟特性（≤ `flush_interval`）。
