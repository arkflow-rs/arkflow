# Design: fix-ack-stall-modes

## Context

两个停摆模式（见 proposal Why 节 file:line 证据）。共同形态：失效被"吞"一次之后，系统进入无错误、无进度的静默停滞——一个在 checkpoint 面向（barrier 等一个复活的计数），一个在 WAL ack 面向（驻留等一个死了的 gap 持有者）。修复都是加"终态/有界"守卫，不改正常路径。

## Goals / Non-Goals

**Goals:**

- abort 终态对迟到 undo 免疫；正常 undo/abort 语义零变化。
- WAL 驻留等待有租约；超时走既有失败栅栏。
- 现有放过缺陷的测试被加深到能抓住它。

**Non-Goals:** 见 proposal Non-goals。

## Decisions

### D1 — `TrackingAck` 增加 `aborted: AtomicBool` 终态标记

`completed` 语义复用导致歧义（ack 完成与 abort 终态都置 true，undo 无法区分）。加独立标记：

```rust
aborted: std::sync::atomic::AtomicBool,
// undo 入口（ack_lock 内）：
if self.aborted.load(Ordering::Acquire) { return Ok(()); }   // 终态 no-op
// abort 入口（ack_lock 内）：
self.aborted.store(true, Ordering::Release);
```

读写都在既有 `ack_lock` 临界区内，无需额外同步。`undo` 的守卫放**入口**（而非 `settle_tracker_after_undo` 内部）：undo 还要调 `inner.undo()`——终态投递不应再扰动 inner（其 inner ack 可能已随 abort 结算/放弃）。与 `FanoutAckPart::undo`（`input/mod.rs:193-197`）的守卫位置对齐。

### D2 — WAL 驻留租约：常量 + 超时分支

```rust
/// Upper bound for a parked WAL acknowledgement waiting for a lower
/// sequence outside the close window.
const WAL_ACK_PARK_TIMEOUT: Duration = Duration::from_secs(60);
```

驻留等待循环（`wal/mod.rs:598-621` 的 `ack_notify` 等待段）从"无限 `notified().await`"改为 `select! { biased; _ = ack_notify.notified() => {}, _ = sleep(PARK_TIMEOUT) => break with timeout error, _ = close.cancelled() => 既有 drain-window 路径 }`。超时错误：`Error::Process("WAL acknowledgement parked for >60s waiting for sequence {first}; the gap owner appears stalled")`——**可重试**（不记 `last_error` 栅栏？不对——要记：gap 持有者已死，后续重试仍会驻留，应 fail-fast）。决策：超时**不**写 `last_error`（那会栅栏住 gap 持有者恢复后的重试，而持有者可能只是慢），但错误返回给驻留调用方 → 链错误处理 abort 该投递 → 恢复重放。若持有者真死，其条目无人结算，流的下一次 ack 仍驻留 → 再次超时 → 持续显式报错（可见的周期性失败，优于静默）。租约值 60s：远大于正常源提交耗时（秒级），远小于 checkpoint 轮 10 分钟界。

### D3 — 测试加深

- `commit.rs`：undo-after-abort 后 `tracker.blocking() == 0` 断言（新增，当前序列无覆盖）。
- `state_journal.rs:2678` `commit_on_ack_undo_after_abort_is_a_noop`：补 tracker 计数断言（该测试的调用序列正是缺陷触发序列，断言加在 tracker 层即可抓回归）。
- `wal/mod.rs`：驻留超时回归（测试用可调 park timeout 注入短值，避免 60s 真等）——实现时把租约做成 `AtomicU64` 可覆盖（与 `ack_drain_window_ms` 同法，`wal/mod.rs:334` 已有先例）。

## Risks / Trade-offs

- **慢源提交被误伤**：60s 租约对正常 ack（ms–s 级）余量充足；唯一的长尾是 object-store 段密封等待（另一变更引入，`flush_interval×4+5s` 上界）——40s 吞吐档（10s interval）下 45s < 60s，不冲突；两变更合入后复核常量关系。
- **周期性显式失败 vs 静默停滞**：持有者真死时从"无错误停摆"变为"每 60s 一次显式错误"——可观测性的净改善，恢复依赖既有重试/重启路径。

## Migration Plan

单 PR，无配置面（常量 + 测试注入）。

## Open Questions

（无）
