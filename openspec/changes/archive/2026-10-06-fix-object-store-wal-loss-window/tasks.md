## 1. 密封门控与可观测性

- [x] 1.1 `crates/arkflow-core/src/wal/store.rs`：`WalStore` trait 增加 `sealed_seq() -> Option<u64>` 与 `seal_notifier() -> Option<&Notify>` 默认方法（默认 `None`）
- [x] 1.2 `crates/arkflow-plugin/src/wal/s3.rs`：`S3WalStore` 增加 `sealed_seq: AtomicU64` + `seal_notify: Notify`；`seal_active_segment` 在 PUT + manifest 更新成功后发布 `sealed_end_seq` 并 `notify_waiters()`；单测：密封后 `sealed_seq()` ≥ 段尾序号、多次密封单调递增
- [x] 1.3 `crates/arkflow-core/src/wal/mod.rs`：`acknowledge` 在源提交前对 `sealed_seq()` 为 `Some` 的后端执行 `wait_for_sealed(seq)`（有界 `flush_interval×4+5s`、取消安全、先检查后等待）；超时沿既有失败栅栏记 `last_error` 并返回错误；redb 路径零变化（`sealed_seq() == None` 跳过）
- [x] 1.4 `crates/arkflow-core/src/wal/mod.rs`：flusher 唤醒分支失败计数（`flush_failures()` 读取器或指标挂点）+ 1 条/10s 限速 warn + 连续 ≥8 次升级 error；关闭分支不动
- [x] 1.5 回归测试：(a) 崩溃窗口——mock store 下 ack 完成后 `sealed_seq ≥ acked seq` 断言、密封前 ack 阻塞、密封后放行；(b) 超时进失败栅栏、重试恢复；(c) `wait_for_sealed` 取消安全（drop 后无泄漏、close 触发即失败可重试）；(d) flusher 持续失败时计数递增、限速 warn、error 升级
- [x] 1.6 `wal_optimization_e2e.rs`：throughput preset 吞吐断言复核，必要时按新 ack 延迟特性调整断言并在断言注释中说明依据（复核结论：该文件仅在 store 层驱动 `append_batch`/`read_after_cursor`，不经过 `Wal::acknowledge`，且为 `#[ignore]` 的 MinIO 门控测试、无 ack 路径吞吐/延迟断言——门控不影响其断言，无需调整）

## 2. 文档与门禁

- [x] 2.1 `docs/docs/`（en）与 zh-Hans 对应页：object-store WAL 的源 ack 门控语义、ack 延迟 ≤ `flush_interval` 的调优说明（`flush_interval`/`max_entries`）
- [x] 2.2 门禁：`cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`；`pnpm docs:check`（统一收口跑：workspace 测试唯一失败为 redis_cluster 环境性端口占用，清理残留 testcontainers 容器后单跑 2/2 通过；clippy 0 警告；fmt 干净；docs:check 通过）
- [x] 2.3 契约复核：delta spec 每个 Scenario 有对应测试证据（1.5 的 (a)–(d)）；tasks 勾选
