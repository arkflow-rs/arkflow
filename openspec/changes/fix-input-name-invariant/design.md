## Context

`MessageBatch.input_name` 是 `multiple_inputs` → join buffer 的数据流契约载体。两条构造路径（`filter_columns` / `new_binary_with_origin`）在产出新批次时走 `From<RecordBatch>` 或 `new_arrow`，均置 `input_name: None`。join buffer 的无名批次路径是 `continue`（trace）——静默数据丢失。

## Goals / Non-Goals

**Goals:** 两条已知路径保留 input_name；join buffer 的无名丢弃可观测（warn + 计数）。

**Non-Goals:** 类型系统级不可变保证、全部构造路径审计（见 proposal）。

## Decisions

**D1 — 保留而非类型化。**
两处构造尾部各加一行 `set_input_name(self.get_input_name())`。`From<RecordBatch>` 不改（它的语义就是"新造"，调用方应自己带名）；只改这两个"派生"方法——它们的语义是"原批次的变换"，input_name 应随之。*备选否决*：加 `filter_columns_with_origin` 新方法 + 弃用旧方法（公共 API 变更，过度）。

**D2 — join buffer 的无名丢弃从 trace 升级为 warn + 计数。**
`continue` 前加 `tracing::warn!(input_name = "missing", "join buffer discarded a batch without an input_name; downstream join data will be incomplete")` + 一个丢弃计数器（`discarded_unnamed_batches: AtomicUsize`，expose 为 pub(crate) 供测试断言）。即使未来新路径漏带名，用户至少在日志和测试中可见。

## Risks / Trade-offs

- [`From<RecordBatch>` 的其他调用方仍可能丢名] → D2 的可观测兜底使这类问题在开发期可发现（warn + 计数），而非生产静默。
- [warn 刷屏] → 无名批次本身就是异常（正常管线全链带名），首例 warn 后每 N 条节流——实现取简：仅 warn 不节流（异常态下刷屏是正确行为）。

## Migration Plan

无配置/格式变更。回滚 = revert。
