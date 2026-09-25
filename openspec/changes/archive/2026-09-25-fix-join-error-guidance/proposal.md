# Proposal: fix-join-error-guidance

## Why

双流 join 当前完全未实现，但两条拒绝路径的报错互相矛盾且形成死路：Stream 编译器拒绝 `join` buffer 时引导用户「declare a Job DAG with an explicit join operator」（`crates/arkflow-core/src/executor/stream_compiler.rs:117`），而 Job DAG 校验恰恰拒绝 `OperatorKind::Join`（`crates/arkflow-core/src/job.rs:574`："Join operator is not supported by the distributed runtime"）。用户撞墙时得到错误导航，且规格文本本身（`stream-config-compilation` 的「migration message pointing to Job DAG configuration」）固化了这处误导。本变更把指引统一为诚实表述，并在文档中显式声明双流 join 边界。这是 PLANNING.md 第八节补齐计划的 P0 项。

## What Changes

1. Stream 编译器对 `join` buffer（含 legacy window `join` 字段）的报错改为诚实表述：双流 join 暂不支持，给出两条可行替代——SQL processor 对临时表的批内 join，或借 Kafka 重分区把两个流共置后单流处理；不再引导用户使用 Job DAG join 算子。
2. Job DAG 校验对 `OperatorKind::Join` 的报错同步措辞，声明同一边界（显式 join 算子待 `add-stream-join-operator` 落地）。
3. 两处报错文案互相引用同一事实（"stream-stream join is not yet supported"），消除矛盾。
4. `docs/docs/concepts/7-distributed-jobs.md` 增加双流 join 边界声明（含替代方案）。
5. 同步修改 `stream-config-compilation` 规格中「migration message」的措辞要求，使规格与诚实指引一致。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `stream-config-compilation`: `join` buffer 拒绝报错的要求从「指向 Job DAG 迁移」改为「诚实声明双流 join 未支持并列出可行替代」。
- `streaming-job-api`: Join 算子拒绝报错的场景补充「与其他拒绝路径指引一致、不引导到不存在的入口」。

## Impact

- `crates/arkflow-core/src/executor/stream_compiler.rs`：两处错误文案与对应测试断言（`join_buffer_rejected_with_migration_message` 等）。
- `crates/arkflow-core/src/job.rs`：Join 拒绝文案与测试断言（`rejects_unsupported_join_operator`）。
- `docs/docs/concepts/7-distributed-jobs.md`：边界声明（en；如有 zh 对应页同步）。
- `openspec/specs/stream-config-compilation/spec.md`、`streaming-job-api/spec.md`：delta 合并后措辞更新。
- 无行为变更：仅错误消息与文档；所有既有测试语义不变（断言文案的测试随文案更新）。
