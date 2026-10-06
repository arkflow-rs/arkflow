# Proposal: fix-ack-stall-modes

## Why

两个"单次被吞的错误 → 管道永久静默停摆"的失效模式：

1. **`TrackingAck::undo` 在 abort 后复活 barrier 阻塞计数**：`crates/arkflow-core/src/executor/commit.rs:445-453` 的 `settle_tracker_after_undo` 无条件 `completed.swap(false)` 并递减 `tracker.completed`——而 `settle_tracker_after_abort`（`commit.rs:459-468`）已把 abort 计为终态结算。时序：投递 D 经 `TrackingAck` T 跟踪 → 错误路径 `T.abort()`（终态结算）→ 缓冲算子的补偿链（窗口 fired 复合 ack 的 held 释放、`ConcurrentAck`/`CommitGroupOnAck` 的 undo 链）晚到一步调用 `T.undo()` → `completed` 被换回 false、`tracker.completed` 递减——**终态死亡的投递重新计入 barrier 阻塞数**。此后每轮 checkpoint 等一个永不结算的投递，10 分钟超时（`kernel_handle.rs:13` 的 `CHECKPOINT_ROUND_TIMEOUT`），**checkpoint 永久失败而数据处理照常**。`undo` 缺少终态检查（对比 `FanoutAckPart::undo`，`input/mod.rs:193-197` 有）；现有测试 `commit_on_ack_undo_after_abort_is_a_noop`（`state_journal.rs:2678-2689`）恰好覆盖这条调用序列，但只断言 journal 层——tracker 层的复活未被抓到。
2. **WAL 驻留 ack 无租约**：`crates/arkflow-core/src/wal/mod.rs:524-548` 中，非最低序号的 ack 调用驻留等待 `ack_notify`，唯一的时间界（`WAL_ACK_DRAIN_WINDOW`，30s）只在 `close.cancel()` 之后生效（`:598-621`）。若 gap 持有者任务死亡且未记录 `last_error`（上游吞错后 drop、sink 错误路径 abort 未重试），驻留者**无限等待**——流无错误、无进度、静默停滞直到停机；`acknowledgements` map 同时每个驻留序号增长一条（无界）。

## What Changes

- `TrackingAck` 增加 `aborted` 终态标记：`abort` 置位后 `undo` 为幂等 no-op（不触碰 tracker 计数）；对齐 `FanoutAckPart::undo` 的既有终态守卫。
- WAL 驻留等待加有界租约：非 close 场景下的驻留等待超过 `WAL_ACK_PARK_TIMEOUT`（默认 60s）即返回可重试错误——不写 `last_error` 栅栏（不栅栏仅仅较慢的 gap 持有者），流以显式周期性错误失败而非静默停滞。
- 修正 `state_journal.rs:2678` 测试使其断言 tracker 层计数（当前只断言 journal 层，放过了本缺陷）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `message-acknowledgment`: ADDED requirement——终态（aborted）投递的后续 undo SHALL 幂等无害，不得复活 barrier 阻塞计数。
- `input-durability`: ADDED requirement——WAL 驻留确认 SHALL 有界等待，超时走失败栅栏而非无限驻留。

## Non-goals

- 不改变 undo/abort 的正常语义与顺序（成功 ack 后的 undo 仍正确回退；abort 幂等保持）。
- 不为 `acknowledgements` map 引入独立容量上限（租约超时 + 失败栅栏已把生命周期收敛到流失败/teardown）。
- 不处理 kernel 报告通道与 checkpoint 轮次超时的其他调优（既有 10 分钟界保持）。

## Impact

- `crates/arkflow-core/src/executor/commit.rs`（TrackingAck 终态守卫）。
- `crates/arkflow-core/src/wal/mod.rs`（驻留等待租约 + 常量）。
- 测试：`commit.rs` 内联（undo-after-abort 计数不变）、`state_journal.rs:2678` 断言加深、`wal/mod.rs` 驻留超时回归。
- 文档：无用户可见配置面（常量级），checkpoint 排障页（en/zh）提一句新的失败模式语义（驻留超时 → 显式错误）。
