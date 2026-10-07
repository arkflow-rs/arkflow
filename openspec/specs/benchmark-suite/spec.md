# Capability: Benchmark Suite

## Purpose

公开可复现的基准套件（issue #87）：一条命令覆盖内核主路径（线性 SQL 聚合、GROUP BY 有状态聚合、过滤投影）、JSON 编解码与状态后端，产出人类可读 markdown 与机器可读 JSON 两种报告。场景全部自包含（无网络、无外部服务）。（源自 `add-public-benchmark` 变更。）
## Requirements
### Requirement: 一条命令可复现的基准

SHALL 提供公开入口 `cargo run -p arkflow --release --example benchmark`，可选参数 `--count`（行数）、`--runs`（测量次数）、`--warmup`（预热次数）、`--json`（机器可读输出）。默认参数 SHALL 在常规开发机上数十秒内完成。全部场景 SHALL 无外部依赖（无网络、无外部服务），任何克隆仓库的贡献者可复现。

#### Scenario: 默认参数完整运行

- **WHEN** 以默认参数运行基准入口
- **THEN** 全部场景完成并输出包含每个场景名称、行数、耗时与吞吐的 markdown 报告

### Requirement: 场景覆盖内核主路径

基准 SHALL 至少覆盖：线性 SQL 聚合管道、GROUP BY 有状态聚合、过滤投影、JSON 编码往返、状态后端读写、Avro schema-registry 解码。流式场景 SHALL 走统一内核公开入口（compile_stream + run_job），与生产执行路径一致；Avro 解码场景 SHALL 走真实 `SchemaRegistryCodec::decode` 路径（离线 in-memory `SchemaResolver`，无网络、无外部服务），并 SHALL 覆盖至少三种 schema 宽度以暴露宽度相关的每消息成本。

#### Scenario: 全部场景产出有限正吞吐

- **WHEN** 以极小行数运行全部场景（smoke）
- **THEN** 每个场景返回有限耗时且吞吐大于零，报告包含全部场景名

#### Scenario: Avro 解码场景自包含

- **WHEN** 在无网络环境运行 avro-decode 场景
- **THEN** 场景经离线 resolver 完成 Confluent wire-format 消息的解码并产出有限耗时与正吞吐，全程不发起任何网络请求

### Requirement: 报告格式稳定

报告 SHALL 同时提供人类可读 markdown 表格与 `--json` 机器可读格式，二者 SHALL 包含相同的场景名、行数、耗时与吞吐字段；基准 SHALL NOT 对吞吐数值做通过/失败断言（性能回归由开发者自行对比，CI 不因机器速度失败）。

#### Scenario: JSON 与 markdown 一致

- **WHEN** 同一次运行分别输出两种格式
- **THEN** 场景集合与吞吐数值一致

### Requirement: CI 基准可观测且不做门禁

CI SHALL 提供基准工作流：push 到 main、手动触发（workflow_dispatch）、以及 PR 携带 `benchmark` 标签时，以 release 构建运行全套基准场景，并把 markdown 报告写入 workflow 的 job summary、把 JSON 报告上传为 artifact（文件名含提交短 SHA，保留期不少于 30 天）。该工作流 SHALL NOT 出现在必需检查（required checks）中，SHALL NOT 对任何吞吐数值做通过/失败断言（既有"报告格式稳定"契约不变）；构建失败或基准崩溃 SHALL 如实反映为 job 失败。

#### Scenario: main 合并自动产出数据点

- **WHEN** 一个提交合并到 main
- **THEN** 基准工作流以 release 模式运行全部场景，run 页面的 job summary 展示 markdown 表格，且可下载含该提交短 SHA 的 JSON 报告 artifact

#### Scenario: PR 标签按需触发

- **WHEN** 一个 PR 被打上 `benchmark` 标签（或带该标签的 PR 收到新推送）
- **THEN** 基准工作流在该 PR 上运行并发布同格式的 summary 与 artifact；未带标签的 PR SHALL NOT 触发

#### Scenario: 数值波动不影响结论

- **WHEN** 某次运行的吞吐数值显著低于历史 run
- **THEN** 工作流结论不受影响（不因机器速度失败）；是否构成回归由人对比报告判断

