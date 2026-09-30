## ADDED Requirements

### Requirement: Join buffer 的无名批次丢弃 SHALL 可观测

join buffer 收到不带 `input_name` 的批次时（数据流契约断裂——`multiple_inputs` 设置的名被中间构造路径丢失），SHALL 以 warn 级日志记录（明示"下游 join 数据将不完整"）并递增丢弃计数器，而非仅 trace 级静默 `continue`。核心 `MessageBatch` 的派生构造方法（列过滤、二进制追加）SHALL 保留原批次的 `input_name`。

#### Scenario: 列过滤后 input_name 保留

- **WHEN** 一个带 `input_name` 的批次经过 `filter_columns`（或 `new_binary_with_origin`）产出新批次
- **THEN** 新批次的 `input_name` 与原批次一致，下游 join buffer 照常注册数据

#### Scenario: 无名批次的丢弃有 warn 与计数

- **WHEN** join buffer 收到一个不带 `input_name` 的批次（契约断裂路径）
- **THEN** 该批次仍被跳过（不 crash），但产生 warn 级日志（"下游 join 数据将不完整"）且丢弃计数器递增——开发期与测试中可断言
