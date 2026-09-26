# Tasks: fix-join-error-guidance

## 1. 错误文案统一

- [x] 1.1 `crates/arkflow-core/src/executor/stream_compiler.rs`：`join` buffer 拒绝文案改为诚实表述（含 `stream-stream join is not yet supported` 短语 + 两条替代），legacy window `join` 字段拒绝同文案。
- [x] 1.2 `crates/arkflow-core/src/job.rs`：`OperatorKind::Join` 拒绝文案同短语、同替代，不再引用「dedicated multi-input Join runtime」。
- [x] 1.3 更新两处内联测试断言（`join_buffer_rejected_with_migration_message`、`rejects_unsupported_join_operator`），断言新短语并断言不再出现「Job DAG」/「dedicated multi-input」误导指引。

## 2. 文档边界声明

- [x] 2.1 `docs/docs/concepts/7-distributed-jobs.md`：补充双流 join 未支持边界与替代方案段落。

## 3. 验证

- [x] 3.1 `cargo test -p arkflow-core` 相关测试通过（join 拒绝两处）。
- [x] 3.2 `cargo test --workspace --all-targets` 全量通过。
- [x] 3.3 `cargo clippy --workspace --all-targets` 无新告警。
