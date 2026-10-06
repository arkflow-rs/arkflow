# Design: fix-object-store-wal-loss-window

## Context

object-store WAL 的现行丢失窗口成因（见 proposal Why 节的 file:line 证据）：`Wal::acknowledge` 在 WAL 游标推进后立即执行源提交，而 object-store 后端的 `advance_cursor` 只更新内存 `acked_hwm`，段密封是 flusher tick 驱动的异步动作——源提交先于段 PUT 即形成"已 ack 但未密封"的崩溃丢失窗口。修复原则：**不动段批机制（吞吐不受影响），只把源 ack 的完成点推迟到段密封之后**——这正是本地 redb 后端 `group_commit` 的既有语义（ack 等 flusher 的持久 commit），两个后端由此对齐。

## Goals / Non-Goals

**Goals:**

- object-store 后端上，源 ack 完成当刻，被 ack 的条目必然已存在于某个已密封段对象中。
- 未密封条目在节点丢失后经源重投恢复（at-least-once），不再静默丢失。
- flusher 唤醒分支的持续失败可通过指标与日志观测，与"正常重试"区分。
- 密封等待有界、取消安全，超时走既有失败栅栏（`last_error`），不引入新的停摆模式。

**Non-Goals:** 见 proposal Non-goals（不改段批参数、不做段回收、不碰 redb 路径与 manifest 协调协议、不加逃逸开关）。

## Decisions

### D1 — 密封等待放在 `Wal::acknowledge`，而不是 `Wal::flush` 或 `read()`

备选对比：

- **`Wal::flush` 同步密封**（`stream_adapter.rs` 的显式 flush 路径）：每条 read 一次 PUT，摧毁段批吞吐——否决。
- **`read()` 发布前密封**：同上，且把持久化策略泄漏进读取路径——否决。
- **`acknowledge` 门控**（采纳）：ack 是"源游标可以推进"的唯一决策点，在此等待密封语义最准确；等待粒度是 flusher 的批间隔（`flush_interval`，balanced 1s / throughput 10s），批内多条 ack 共享同一次密封——与 redb `group_commit` 的 fsync 批语义同构。

实现：`acknowledge` 在游标推进、源提交执行**之前**，object_store 分支增加密封等待：

```rust
// wal/mod.rs — acknowledge, before running the source commit
if self.store.kind() == "object_store" {
    self.wait_for_sealed(seq).await?;   // bounded, cancel-safe
}
```

`wait_for_sealed` 循环 `select! { biased; notify, timeout, close.cancelled() }`：每轮先读 `sealed_seq`（`AtomicU64`，Relaxed 足够——该值只作单向单调进度提示，最终一致即正确），已覆盖则返回；否则等待密封 `Notify` 或超时。超时界取 `flush_interval × 4 + 5s`（容忍一次 put 重试），超时返回错误——沿既有失败栅栏：记录 `last_error`，条目保持可重试，恢复路径重放。`close.cancelled()` 触发时按既有 `WAL_ACK_DRAIN_WINDOW` 语义处理（不密封即失败，恢复重放）。

### D2 — `sealed_seq` 由 `S3WalStore` 发布，core 经 trait 扩展读取

`WalStore` trait（`crates/arkflow-core/src/wal/store.rs`）增加默认方法：

```rust
/// Highest sequence guaranteed contained in a sealed segment object.
/// Default `None` = backend seals synchronously inside append/flush (redb).
fn sealed_seq(&self) -> Option<u64> { None }
/// Notification fired after each successful seal. Default no-op.
fn seal_notifier(&self) -> Option<&tokio::sync::Notify> { None }
```

redb 返回 `None`（`flush_pending` 即持久 commit，无需门控——`acknowledge` 走原路径零变化）；`S3WalStore` 在 `seal_active_segment`（`s3.rs:979-1007`）PUT 成功且 manifest 更新返回后更新 `sealed_seq = sealed_end_seq` 并 `notify_waiters()`。**发布点在 manifest 写完成后**：恢复依赖 LIST 兜底（`s3-wal-pipeline` D5），仅 PUT 成功而 manifest 失败的段恢复也能重放，但按更保守的"manifest 完成才算密封"发布，保证 ack 完成当刻 manifest 一定已知该段（收紧恢复确定性）。

`acknowledge` 的门控条件：`store.sealed_seq()` 为 `None` 时跳过（本地后端），否则等待 `sealed_seq >= seq`。

### D3 — flusher 可见性：计数 + 限速 warn + 升级 error

`wal/mod.rs:376` 唤醒分支：失败时 `flush_failure_count += 1`（`AtomicU64`，经 `KernelMetrics`/`ChainMetrics` 相邻的既有指标通路暴露——若 WAL 无指标挂点则在 `Wal` 上暴露 `flush_failures()` 读取器，由 `WalInput` 的状态上报带出）；warn 按 1 条/10s 限速（对齐 `event_time_gate.rs` 的 `WARN_INTERVAL` 惯例）；连续失败 ≥ 阈值（默认 8，约一个 `flush_interval` 数量级）升级 error 并带上最近错误。关闭分支（`wal/mod.rs:372`）不动——`close()` 已重新上抛。

## Risks / Trade-offs

- **ack 延迟增加 ≤ `flush_interval`**：源链的 ack 完成点后移，in-flight 窗口（通道容量 1024）内的吞吐由每密封批的条目数决定，与 redb `group_commit` 同构。`wal_optimization_e2e.rs` 的吞吐断言需复核；若 throughput preset 下成为瓶颈，正确调法是调 `flush_interval`/`max_entries`，不是绕过门控（已在 docs 任务中写明）。
- **flusher 停摆传导为 ack 超时错误**：密封等待超时会让源 ack 失败并进入失败栅栏（fail-closed，可重试），比静默丢失更安全，但需依赖 D3 的可观测性区分"flusher 慢"与"flusher 死"。
- **取消安全**：`wait_for_sealed` 每轮先检查后等待（检查-等待之间用 `notify_waiters` 的全量唤醒语义兜底——`Notify::notified` 在 `notify_waiters` 后新建的等待会立即返回，不存在丢失唤醒）；超时与 close 均有界。

## Migration Plan

单 PR 落地，行为变更即时生效（无配置迁移）：object-store WAL 用户升级后源 ack 延迟 +≤`flush_interval`，丢失窗口消失。`s3.rs` 51 例既有测试与 `wal/mod.rs` 回归全绿；新增崩溃窗口回归测试（见 tasks 1.4）。

## Open Questions

（无——密封发布点选 manifest 完成后已在 D2 定案；若实测 throughput preset 下 ack 延迟不可接受，后续以 `flush_interval` 调优指南跟进，不改协议。）
