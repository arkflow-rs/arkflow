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

基准 SHALL 至少覆盖：线性 SQL 聚合管道、GROUP BY 有状态聚合、过滤投影、JSON 编解码往返、状态后端读写。流式场景 SHALL 走统一内核公开入口（compile_stream + run_job），与生产执行路径一致。

#### Scenario: 全部场景产出有限正吞吐

- **WHEN** 以极小行数运行全部场景（smoke）
- **THEN** 每个场景返回有限耗时且吞吐大于零，报告包含全部场景名

### Requirement: 报告格式稳定

报告 SHALL 同时提供人类可读 markdown 表格与 `--json` 机器可读格式，二者 SHALL 包含相同的场景名、行数、耗时与吞吐字段；基准 SHALL NOT 对吞吐数值做通过/失败断言（性能回归由开发者自行对比，CI 不因机器速度失败）。

#### Scenario: JSON 与 markdown 一致

- **WHEN** 同一次运行分别输出两种格式
- **THEN** 场景集合与吞吐数值一致
