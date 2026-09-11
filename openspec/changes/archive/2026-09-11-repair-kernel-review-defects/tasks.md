# Tasks: repair-kernel-review-defects

## 1. Kernel checkpoint participation

- [x] 1.1 Add `finished_reporter: Option<tokio::sync::mpsc::UnboundedSender<String>>` to `CheckpointHook` (`crates/arkflow-core/src/executor/task.rs:36`) and send the entry task id from `run_chain` immediately after `run_chain_inner` returns, on every exit path.
- [x] 1.2 Wire the finished channel through `KernelJobRunner::spawn_with_cancellation_mode` (`crates/arkflow-core/src/executor/kernel_handle.rs:527`): one sender clone per hook, receiver plus one keep-alive sender stored on `KernelJobHandle`.
- [x] 1.3 In `checkpoint_barrier_inner` (`crates/arkflow-core/src/executor/kernel_handle.rs:138`): drain ended chain ids at round start, skip barrier injection for ended source chains, wait on `participants - ended`, accept late reports from chains that exited after reporting, and keep the loud error when no participant remains.

## 2. Worker pool fence ordering

- [x] 2.1 Rework `ProcessorWorkerPool::flush` (`crates/arkflow-core/src/executor/task.rs:1767`): create + pin + `enable()` the `Notified` future before reading `flushed`, then select on the pinned future.
- [x] 2.2 Fence the idle-tick branch (`crates/arkflow-core/src/executor/task.rs:1176`) with `pool.flush().await?` before `tick_chain`, mirroring the barrier path.

## 3. Regression tests

- [x] 3.1 `bounded_source_drain_keeps_checkpoints_running` (`crates/arkflow-core/src/executor/tests.rs`): a two-source Job where one bounded source ends; assert a checkpoint round started after the drain completes and carries the live source's position.
- [x] 3.2 Pooled-chain tick ordering test: `pipeline.thread_num > 1` with a slow processor and an `on_tick`-generating processor; assert tick output never precedes previously submitted data at the sink.
- [x] 3.3 Concurrency stress on `flush`: rapid barriers against a slow pooled processor; assert every barrier fence returns within a timeout (no lost wakeup).

## 4. Spec deltas (drafted in this change; verify at archive)

- [x] 4.1 `openspec validate repair-kernel-review-defects --strict --no-interactive` passes.
- [x] 4.2 Each delta's `### Requirement:` header matches its master spec verbatim.

## 5. Final verification

- [x] 5.1 `cargo fmt --all -- --check` and `git diff --check` are clean.
- [x] 5.2 `cargo test -p arkflow-core --lib executor::` passes.
- [x] 5.3 `cargo clippy -p arkflow-core --all-targets` reports no new warnings in touched files.
