## Why

P2 core 批（`openspec/CODE_REVIEW_2026-09-29.md`「input_name 不变量丢失」，core 深潜核实）。`MessageBatch.input_name` 是跨组件契约的载体——`multiple_inputs` 输入设置它，join buffer 依赖 `get_input_name()` 注册 DataFusion 临时表，**无名批次被静默丢弃**（`buffer/join.rs:76-79` 的 `let Some(input_name) = ... else { continue; }`——只有 trace 级日志）。

两条 `MessageBatch` 构造路径会把它丢掉：

1. **`filter_columns`**（`lib.rs:357-381`）：最后 `Ok(batch.into())` 走 `From<RecordBatch>`（`:440-447`）——`input_name: None`。任何 processor 中途调用列过滤（如 SQL projection、schema 精简），下游 join 的数据就**静默消失**——无错误、无 warn、只有 trace。
2. **`new_binary_with_origin`**（`lib.rs:333-356`）：`MessageBatch::new_arrow(new_msg)`（`:388-393`）——同样 `input_name: None`。

这不只是"字段丢了"——join 的语义正确性被静默破坏：一侧数据无声缺席，join 结果看起来"正常"但少了行，用户无从得知。

## What Changes

- `filter_columns` 和 `new_binary_with_origin` 保留原批次的 `input_name`（构造后 `set_input_name(self.get_input_name())`——一行修一处）。
- join buffer 对无名批次的处理从 `continue`（trace）升级为 **warn + 计数**——即使未来有新路径漏带 input_name，用户至少能在日志/指标中看到数据被丢弃（而不是无声缺席）。
- 测试：`filter_columns`/`new_binary_with_origin` 保留 input_name；join buffer 收到无名批次时 warn（计数器断言）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `stream-join-operator`：join buffer 的无名批次处理 SHALL 可观测（warn + 指标，非 trace + 静默丢弃）。

## Impact

- `crates/arkflow-core/src/lib.rs`（两处构造路径保留 input_name）
- `crates/arkflow-plugin/src/buffer/join.rs`（无名批次 warn + 计数）
- 无配置面/存储格式变更。

## Non-goals

- 不把 `input_name` 编译进类型系统（`MessageBatch` 的字段级不可变保证需更大的 API 重设计）。
- 不审查全部 MessageBatch 构造路径（只修已核实的两条丢名路径；未来新路径由 join buffer 的可观测丢弃兜底发现）。
