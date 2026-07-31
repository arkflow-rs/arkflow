## Why

S3 WAL 后端的 committed cursor 不跟踪 ack。`S3Store::advance_cursor(seq)` 在 `crates/arkflow-plugin/src/wal/s3.rs:643-658` 直接 `let _ = seq;` 丢弃传入的序列号，cursor 的推进完全脱离「哪条消息被 ack」，只由 `flush_manifest`（`crates/arkflow-plugin/src/wal/s3.rs:881-908`）从 active segment 的 `last_seq` 推导并 clamp 到 `max_sealed_seq`。而 `WalAck::ack`（`crates/arkflow-core/src/wal/mod.rs:450-457`）确实把 seq 一路传到了 store——是 store 层把它扔了。

后果已由复现测试 `s3_cursor_does_not_track_acked_seq_after_restart`（`crates/arkflow-plugin/src/wal/s3.rs` 测试模块，用 InMemory 后端）坐实，当前 FAILING：

1. 写入 seq 1..=5 并全部 `advance_cursor` 后重启，`next_seq_hint()` 返回 1（`crates/arkflow-plugin/src/wal/s3.rs:747-754`，即 `cursor()+1`）而非 6，下一次 append 复用已存在的序列号——与 redb 后端的 `max_seq()+1`（`crates/arkflow-core/src/wal/store.rs:372-380`）行为不一致。
2. `read_after_cursor()` 返回全部「已 ack」的 5 条，每次重启重复重放，把 at-least-once 放大为无限重复。

这是 #1183 引入 S3 backend 时遗留的正确性缺陷（`advance_cursor` 的占位实现从未接上 seq），#1186/#1191/#1192 的并发与 manifest 协调优化都没碰到它。现在修，是为了让 S3 后端真正满足 `input-durability` 承诺的 at-least-once 语义，而不是在一个会丢/重放 cursor 的底层上继续叠加能力。

## What Changes

- **`S3Store::advance_cursor(seq)`** 不再丢弃 seq：`S3Store` 新增一个 `AtomicU64` ack 高水位字段，`advance_cursor` 用 `fetch_max(seq)` 记录已确认的最高序列号，再按既有阈值（`cursor_cfg.max_entries` / `interval`）触发 `flush_manifest`（删掉 `let _ = seq;`）。
- **`flush_manifest`** 的 manifest mutator 改为 `m.cursor = max(m.cursor, acked_hwm.min(m.max_sealed_seq))`：cursor 只能推进到「已 seal 落盘」的最高序列号，杜绝推进过头使未落盘条目被跳过（at-least-once 退化为丢数据）。
- **`S3Store::next_seq_hint()`** 改为返回 `max(max_sealed_seq, active.last_seq) + 1`（对齐 redb 的 `max_seq()+1`），杜绝重启后序列号复用。
- **回归测试**：现有复现测试 `s3_cursor_does_not_track_acked_seq_after_restart` 转绿；新增「部分 ack + 重启后新 append 不复用 sealed 但未 ack 的序列号」场景测试。

非破坏性变更：manifest JSON schema 不变；cursor 推进的对外语义（at-least-once）不变，只是从「错误」变为「正确」。

## Capabilities

### New Capabilities
<!-- 无 -->

### Modified Capabilities
- `s3-wal-pipeline`: 新增 Requirement「Committed cursor tracks acknowledgements」及其 WHEN/THEN Scenario——约束 S3 后端 `advance_cursor` 必须记录 ack 序列号、cursor 推进不得越过已落盘的 `max_sealed_seq`、`next_seq_hint` 必须基于「已写最高序列号」而非 cursor。该 capability 现行的 failure/recovery 章节覆盖了 PUT 失败与 manifest 并发协调，但未覆盖「cursor 与 ack 脱钩」这一被忽视的正确性维度。

## Impact

**Affected code:**
- `crates/arkflow-plugin/src/wal/s3.rs` — `advance_cursor`、`flush_manifest`、`next_seq_hint`、`S3Store` 新增 ack 高水位字段及其在 `build_with_client` / `recover` 中的初始化；测试模块新增/转绿回归测试。
- `openspec/specs/s3-wal-pipeline/spec.md` — 新增一条 Requirement（delta 形式）。

**API changes:** 无公共 API 变化；`WalStore` trait 签名不变；`WalConfig` 字段不变。

**Performance impact:** 可忽略——`fetch_max` 是单次原子操作；`flush_manifest` 仍是按阈值的批量 PUT，触发频率不变。

## Non-goals

- 不改 redb（local）后端——其 `advance_cursor` / `next_seq_hint` 已正确。
- 不引入 exactly-once 语义——仍是 at-least-once，去重责任在下游。
- 不改 manifest JSON schema、不改 segment 编码格式。
- 不实现 retention / 已 seal segment 的回收（D7），属独立工作。
- 不重构 `WalStore` trait。
