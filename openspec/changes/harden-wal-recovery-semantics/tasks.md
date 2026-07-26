## 1. 重构 recovery 控制流

- [x] 1.1 在 `crates/arkflow-core/src/stream/mod.rs` 的 `Stream::run` 中，在 `self.input.connect()` / `self.output.connect()` / temporaries 连接全部成功之后、`tracker.spawn(Self::do_input(...))` 之前，插入 recovery 块：调用 `self.wal.clone()` 的 `read_after_cursor()`，遍历 entries 用 `WalAck::new(wal, seq, Arc::new(NoopAck))` 包装后调用 `Self::forward(msg, ack, &self.buffer, &input_sender).await`，所有错误 `?` 上抛。验证：编译通过；`cargo test -p arkflow-core --lib stream::tests::stream_close_flushes_group_commit_pending` 仍然通过（happy path 不破）。
- [x] 1.2 从 `Stream::do_input` 中删除原来的 recovery 块（`stream/mod.rs:181-201` 的 `if let Some(wal) = &wal { ... }` 整段），同时从 `do_input` 的参数列表中移除不再需要的 `wal: Option<Arc<Wal>>`（如果 do_input 的其它路径仍然需要 wal——例如 append 路径——则保留参数；只删 recovery 块）。验证：编译通过；`cargo check -p arkflow-core`。

## 2. 让 Stream::run 在错误路径上也关闭资源

- [x] 2.1 检查 `Stream::run` 当前结构，确保 recovery 失败时已经 connect 的资源（input/output/error_output/temporaries/WAL）都被 close。如果当前是直接 `?` 上抛，改为 `let result = recovery_and_run_inner().await; self.close().await?; result` 模式，或者用 `match`/`try` 块确保 `self.close()` 在错误路径上仍跑。验证：阅读改动，确认每条错误返回路径都触发 `self.close()`。

## 3. 新增回归测试

- [x] 3.1 ~~在 `crates/arkflow-core/src/stream/mod.rs` 的 `tests` 模块里新增测试 `stream_run_returns_err_when_wal_read_fails`~~ **改为 `wal_corruption_surfaces_before_stream_run`**：原计划"corrupt 后让 Stream::run 触发 read_after_cursor 失败"在 redb 2.6.3 下不可靠（redb 在 `Database::open` 时已经 `verify_primary_checksums` 走完整棵树，corruption 会被 open-time 检测到，read_after_cursor 路径触发不到；且 redb 有自己的 read cache，corrupt 文件后 mmap 行为不确定）。改为：测试 corrupt WAL 文件后 `Wal::open` 失败，这正是 `StreamConfig::build` 会暴露的错误——stream 永远到不了 `run()`。设计文档里 `?` propagation 的覆盖由 3.2 间接验证（同样的 `?` 上抛机制）。验证：`cargo test -p arkflow-core --lib stream::tests::wal_corruption_surfaces_before_stream_run` 通过。
- [x] 3.2 在 `crates/arkflow-core/src/stream/mod.rs` 的 `tests` 模块里新增测试 `stream_run_returns_err_when_recovery_forward_fails`：用 `FailingBuffer`（`Buffer::write` 永远返回 `Err`）模拟 recovery forward 失败；先直接调 `Wal::append` 写一条 unacked entry；验证 `Stream::run` 返回 `Err`、cursor 没有前进、StubInput 的 read 没被调用（worker 没 spawn）。验证：测试通过。
- [x] 3.3 在 `crates/arkflow-core/src/stream/mod.rs` 的 `tests` 模块里新增端到端测试 `stream_run_replays_unacked_entries_before_new_input`，覆盖 spec scenario 4（"Normal recovery still works"）：Phase 1 用 `Wal::append` 写一条 unacked entry 后关闭；Phase 2 重开 WAL + 装一条 fresh input；用 `RecordingOutput` 记录 output 收到的 value 顺序。验证：(a) `Stream::run` 正常结束；(b) output 收到的顺序是 `[REPLAYED_VALUE, NEW_VALUE]`，证明 replay 先于新 input；(c) cursor 推进到 2（replay 一条 + 新 input 一条，都通过 WalAck ack）。新增 helper `sample_batch_with_value(v: i64)`（参数化原 `sample_batch`）。验证：`cargo test -p arkflow-core --lib stream::tests::stream_run_replays_unacked_entries_before_new_input` 通过；现有测试不破。

## 4. 文档与 changelog

- [x] 4.1 更新 `openspec/specs/input-durability/spec.md`，把本 change 的 `## ADDED Requirements` 同步到主 spec（这一步在 archive 时由 openspec 工具完成，但需在 PR 描述中明确指出"spec 已同步"）。
- [x] 4.2 在 `docs/docs/components/0-inputs/delivery-semantics.md` 末尾加一段"WAL recovery 失败时的行为"，说明 stream 会启动失败而非降级运行，运营需要监控 stream 启动失败并准备人工介入或回滚 WAL。验证：`mkdocs serve` 本地预览正常（如果有 docs 构建）。

## 5. 验证与提交

- [x] 5.1 `cargo test --workspace` 全绿，包括新增的三个测试（`wal_corruption_surfaces_before_stream_run`、`stream_run_returns_err_when_recovery_forward_fails`、`stream_run_replays_unacked_entries_before_new_input`）和现有的 `stream_close_flushes_group_commit_pending`、`crash_recovery_replays_unacked_then_advances_on_ack`、`corrupted_store_surfaces_error`。
- [x] 5.2 `cargo clippy --workspace --all-targets -- -D warnings` 无新增 warning（所有 clippy 报错均为 pre-existing，与本 change 无关）。
- [x] 5.3 `cargo fmt --check` 通过（本 change 涉及的 `stream/mod.rs` 已 fmt-clean；其它文件的 pre-existing fmt 问题不在本 change scope 内）。
- [ ] 5.4 PR 描述中包含 BREAKING 行为说明：WAL 损坏或 recovery 阶段下游不可用时，stream 行为从"继续运行+log"变为"启动失败"，依赖 k8s/systemd 重启策略。
