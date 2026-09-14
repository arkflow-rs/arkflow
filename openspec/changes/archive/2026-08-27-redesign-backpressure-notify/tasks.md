## 1. 在 Stream 引入 backpressure 信号

- [x] 1.1 在 `Stream` 结构体新增字段 `next_seq_notify: Arc<tokio::sync::Notify>`，与 `sequence_counter` / `next_seq` 并列。
- [x] 1.2 在 `Stream::new` 初始化 `next_seq_notify: Arc::new(Notify::new())`；在 `use` 区补 `use tokio::sync::Notify;`（Decision 4）。
- [x] 1.3 Verify：`cargo build -p arkflow-core` 成功（2.32s）。`tokio` 的 `full` feature 已含 `sync`，无需改 `Cargo.toml`。

## 2. `do_output` 推进 `next_seq` 后发信号

- [x] 2.1 给 `do_output` 增加 `next_seq_notify: Arc<Notify>` 参数；在 `next_seq.fetch_add(1, Release)` 之后紧跟 `next_seq_notify.notify_one();`（Decision 2）。
- [x] 2.2 在 `run_inner` 调用 `do_output` 处传入 `self.next_seq_notify.clone()`。
- [x] 2.3 Verify：`cargo build -p arkflow-core` 成功；clippy 对本改动的 stream/mod.rs 部分无新增 warning（见 5.2 的 pre-existing 注记）。

## 3. `do_processor` 的 backpressure 改为 check-then-await

- [x] 3.1 给 `do_processor` 增加 `next_seq_notify: Arc<Notify>` 参数；在 `run_inner` 调用处传入 `self.next_seq_notify.clone()`。
- [x] 3.2 将 backpressure 块按 Decision 3 重写：先 `let notified = next_seq_notify.notified();`，再 `load` 两量算 `pending`；若 `pending > BACKPRESSURE_THRESHOLD` 则 `notified.await;` 后 `continue;`，否则 `drop(notified);` 落到既有 `recv_async`。**移除** `wait_time` 计算与 `tokio::time::sleep(...)` 调用。
- [x] 3.3 核对 `use` 区无 orphan import；`do_input` 的重连退避 `sleep` 未动。
- [x] 3.4 Verify：`cargo build -p arkflow-core` 成功；`drop(notified)` 未触发任何 clippy lint（`Notified` 实现 `Drop`，不报 `drop_non_drop`）。

## 4. Backpressure 回归测试

> 实现说明：原计划的「门控 mock output + 端到端驱动」改为对 `do_processor` / `do_output` 的**直接单元测试**。原因——端到端触发 backpressure 需 `strand 异速`（一个 processor 卡住 `next_seq`、其余狂奔领号），构造困难且 flaky；直接单测能精确、稳定地覆盖 spec 三条 requirement + 活跃性。两个测试加入 `stream::tests`：`processor_worker_waits_on_notify_under_backpressure`、`output_worker_notifies_on_next_seq_advance`。

- [x] 4.1 `processor_worker_waits_on_notify_under_backpressure`：构造 `sequence_counter=2×THRESHOLD`、`next_seq=0`（in-flight 超阈值），断言 processor 不消费输入（`output_rx.is_empty()`）；推进 `next_seq` + `notify_one()` 后断言即时恢复并产出；`drop(input_tx)` 后断言 worker 在 timeout 内观察 EOF 退出（无死锁）——覆盖 spec「In-flight 上界保持有界」「Backpressure 解除为信号驱动」「活跃性」。
- [x] 4.2 `output_worker_notifies_on_next_seq_advance`：投递 `seq=0 == next_seq` 的 batch，断言 `notify` 被触发且 `next_seq` 推进到 1——覆盖信号源端语义。按序输出由既有 `output_worker_calls_write_batch_once_per_ack_range` 等测试持续守护。
- [x] 4.3 时序断言用 `is_empty()` / `timeout` / 计数器，仅在「等待 worker 进入 backpressure」处用一次 100ms sleep（测试设施，非被测机制）。
- [x] 4.4 活跃性由 4.1 的 EOF 退出分支覆盖。
- [x] 4.5 Verify：`cargo test -p arkflow-core` 全过（146 passed; 0 failed），含两个新测试。

## 5. 全量验证

- [x] 5.1 `cargo test --workspace --lib` 全绿：arkflow-plugin 221 + arkflow-core 146 passed; 0 failed。需外部服务（kafka/postgres/…）的 plugin 集成测试按惯例在 CI 跑，本地跳过；本改动仅触及 `stream/mod.rs` 内部，不影响 plugin 对 `Input/Output/Processor` trait 的实现。
- [x] 5.2 `cargo clippy -p arkflow-core`：本改动（`Notify` / check-then-await / `drop(notified)` / 两个测试）**零新增 warning**。stream/mod.rs 上的 2 个 warning 为 **pre-existing**（`Stream::new` too_many_arguments、`do_output` 内层 loop 的 while_let_loop），经 `git stash` 对比确认改动前后数量不变，不在本次范围。
- [x] 5.3 `cargo fmt -p arkflow-core -- --check`：本改动干净。stream/mod.rs 与 codec/mod.rs 的 2 处 diff 为 **pre-existing**（`git stash` 对比确认），不在本次范围。
- [x] 5.4 手动核对：`do_processor` 已无用于 backpressure 的 `tokio::time::sleep`（仅 `do_input` 重连退避 `:279` 与测试内的等待保留）；`do_output` 每次 `next_seq.fetch_add`（`:444`）后均调用 `notify_one()`（`:445`）。

## 6. OpenSpec 校验与归档

- [x] 6.1 `openspec status`：4/4 artifact done；`openspec validate redesign-backpressure-notify --strict` 通过。
- [x] 6.2 已归档（2026-08-27）：`specs/stream-backpressure/spec.md` 已同步进 `openspec/specs/stream-backpressure/`。
