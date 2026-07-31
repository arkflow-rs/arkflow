## Context

S3 WAL 后端（`crates/arkflow-plugin/src/wal/s3.rs`）的 committed cursor 当前不跟踪 ack：

- `S3Store::advance_cursor(seq)`（`s3.rs:643-658`）执行 `let _ = seq;`，丢弃传入序列号；只累加 `cursor_pending` 计数，按阈值触发 `flush_manifest`。
- `flush_manifest`（`s3.rs:881-908`）的 manifest mutator 用 `m.cursor = active.last_seq.min(m.max_sealed_seq)` 推进 cursor——值来自 active segment 的 `last_seq`，与「哪条被 ack」无关。
- `next_seq_hint()`（`s3.rs:747-754`）返回 `cursor()+1`。

对比之下，redb 后端（`crates/arkflow-core/src/wal/store.rs:307,372`）的 `advance_cursor` 精确写入 seq、`next_seq_hint` 用 `max_seq()+1`，行为正确。`WalAck::ack`（`crates/arkflow-core/src/wal/mod.rs:450-457`）把 seq 一路传到 store，是 store 层丢了它。

复现测试 `s3_cursor_does_not_track_acked_seq_after_restart`（InMemory 后端）当前 FAILING：写入 seq 1..=5 全部 ack 后重启，`next_seq_hint()==1`（应 6）、`read_after_cursor()` 返回 5 条（应空）。

约束：manifest JSON schema 不能变（Non-goal）；仍是 at-least-once；改动须 surgical，只动 `s3.rs`。

## Goals / Non-Goals

**Goals:**
- 让 S3 后端的 committed cursor 真正跟踪 ack，使已确认且已落盘的条目在重启后不再被重放。
- 让 `next_seq_hint` 基于「已写最高序列号」，杜绝重启后序列号复用。
- 与 redb 后端在 cursor/seq 语义上对齐。

**Non-Goals:**
- 不改 redb 后端（已正确）、不改 `WalStore` trait、不改 manifest schema。
- 不引入 exactly-once（仍 at-least-once，去重在下游）。
- 不做 retention（D7）。

## Decisions

### D1：ack 高水位用 `AtomicU64` + `fetch_max`，不持锁
`advance_cursor(seq)` 是 ack 热路径（每条确认消息都走），必须廉价。新增 `acked_hwm: AtomicU64` 字段，`advance_cursor` 用 `acked_hwm.fetch_max(seq, AcqRel)` 记录，删掉 `let _ = seq;`。`fetch_max` 保证单调，与现有 `cursor_pending: AtomicU64` 风格一致。
- **备选**：用 `active` mutex 保护一个 `u64`——会和 `append_batch`/`seal_active_segment` 抢同一把锁，且 `advance_cursor` 当前已 `lock().unwrap()` 了 `active` 又立即 drop（`s3.rs:647`），改为独立原子更干净。否决。

### D2：`flush_manifest` 把 cursor clamp 到 `max_sealed_seq`
mutator 改为 `m.cursor = max(m.cursor, acked_hwm.min(m.max_sealed_seq))`。`max_sealed_seq` 是「已 PUT 到 object store 的最高序列号」，是 cursor 的**安全上界**：cursor 推进到尚未落盘的 seq 会让重启跳过该条（丢数据）。被 clamp 掉的「ack 了但未 seal」部分在下次 seal 后推进，且这些条目仍在 active/segment 里、重启会重放——at-least-once 成立。
- **备选**：seal-on-ack（每次 ack 都 seal 落盘以即时推进 cursor）——违背 group_commit 的批量化初衷，引发 PUT 风暴。否决。

### D3：`next_seq_hint` 用已写最高序列号 +1
改为 `max(max_sealed_seq, active.last_seq) + 1`（对齐 redb 的 `max_seq()+1`）。重启后若存在「sealed 但未 ack」条目（`cursor < max_sealed_seq`），`cursor()+1` 会复用序列号；用已写最高 seq+1 杜绝复用。`recover()`（`s3.rs:534`）已算出 `max_seq_seen`，把它落到 store 的一个字段（`max_written_seq`）供 `next_seq_hint` 取用即可，无需在 hint 时再遍历 object store。运行期该缓存由 `append_batch` 写入新条目时 `fetch_max` 更新——它是所有新 seq 的唯一入口，已覆盖 active 与 sealed，故无需在 seal 路径重复更新（seal 只搬运 append 已见的 seq）。
- **备选**：在 `next_seq_hint` 里 LIST segments 求最大——S3 LIST 昂贵（100-500ms），且 recover 已遍历过。否决，复用 recover 结果。

### D4：ack 高水位纯内存，不持久化
`acked_hwm` 只在 `flush_manifest` 时折算进 manifest 的 cursor，自身不入 manifest。重启后 `acked_hwm` 重置为 0，但 cursor 已持久化推进结果；未 flush 的 ack 在重启后表现为「重放」（at-least-once 重复），不丢数据。这与 group_commit 的 loss window 语义一致，且无需改 manifest schema。

## Risks / Trade-offs

- **[未 flush 的 ack 在重启时丢失]** → 重放已 ack 条目（at-least-once 重复），不丢数据；与 group_commit 既有 loss window 同语义。可接受。
- **[cursor clamp 使「ack 了但未 seal」的推进延迟到下次 seal]** → 期间重启会重放这些条目，仍 at-least-once；不丢。
- **[`next_seq_hint` 依赖 recover 缓存的 max_seq]** → 若 reopen 路径 max_seq 计算有误则 hint 不准。Mitigation：复用 `recover()` 已正确计算的 `max_seq_seen`（`s3.rs:534`），同一来源；并加「部分 ack 重启不复用序列号」回归测试覆盖。
- **[存量 WAL 首次升级]** → 无 schema 变化，旧 manifest 直接兼容；首次重启后 cursor 从错误值逐步自愈（已 ack 且已 seal 的部分被正确推进，旧的重复重放在重新 ack 后消失）。

## Migration Plan

- 无数据迁移、无 schema 变化。部署后第一次重启即生效。
- **回滚**：还原 `s3.rs` 中 `advance_cursor`/`flush_manifest`/`next_seq_hint` 三个函数及新增字段即可，无残留状态。
