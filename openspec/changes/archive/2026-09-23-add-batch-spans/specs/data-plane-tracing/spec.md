# data-plane-tracing 变更（Delta）

## ADDED Requirements

### Requirement: batch 级处理 span

追踪启用时，统一内核 SHALL 为 source chain 中每个从 source 读取的 batch 创建 `chain.batch` 子 span（parent 为 chain.run），属性含 `rows`（batch 行数）和 `task`（链入口任务 id）。span SHALL 覆盖从 batch 进入 pipeline 到发送 downstream 的全程。关闭追踪时不产生任何额外开销。

#### Scenario: batch span 包含 rows 和 task 属性

- **WHEN** source chain 读取一个 10 行的 batch
- **THEN** 导出的 chain.batch span 包含 `rows=10` 和 `task=<入口任务id>` 属性

#### Scenario: 关闭追踪时零开销

- **WHEN** OTel tracing 未启用
- **THEN** batch 处理路径无 span 创建开销（tracing subscriber 丢弃 span）
