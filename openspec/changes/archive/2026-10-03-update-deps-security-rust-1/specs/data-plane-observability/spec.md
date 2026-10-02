## ADDED Requirements

### Requirement: 指标栈依赖升级保持导出契约

prometheus 客户端依赖升级（0.13 → 0.14，含其 protobuf 后端 rust-protobuf 2.x → 3.x）SHALL 保持 Prometheus 文本导出契约不变：exposition 采用文本格式 0.0.4（含 HELP/TYPE 元数据行）、指标族/序列命名（`arkflow_job_*`、legacy `arkflow_stream_*`）、闭合标签词汇表（`node`/`job`/`chain`/`stream_id`）与数值渲染逐项与升级前一致。升级引入的依赖版本 SHALL 消除 CVE-2025-53605（protobuf 2.28.0 递归崩溃）对应的告警。

#### Scenario: exposition 文本与升级前一致

- **WHEN** 同一 Kernel Job / Stream 指标快照在升级前后分别渲染为 Prometheus 文本
- **THEN** 两次输出的 HELP/TYPE 行、样本行（指标名、标签集、值）与非空序列集合完全一致

#### Scenario: 指标端点行为不受依赖升级影响

- **WHEN** 抓取本地数据面指标端点（或 Hub 聚合导出，含 `node` 标签）
- **THEN** 返回的 exposition 可被标准 Prometheus 解析器解析，序列词汇表不出现 `node`/`job`/`chain`/`stream_id` 之外的标签

#### Scenario: 漏洞依赖从图中消失

- **WHEN** 升级落地后检查依赖图
- **THEN** `protobuf` 2.x 不再出现（prometheus 0.14 解析到 `protobuf ^3.7.2`），dependabot 对应告警（#3，CVE-2025-53605）重扫描后关闭
