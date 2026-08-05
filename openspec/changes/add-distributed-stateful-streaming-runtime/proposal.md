## Why

ArkFlow 当前的 `Stream` 运行时以本地 bounded channel、`thread_num` 处理器和顺序输出为核心（`crates/arkflow-core/src/stream/mod.rs:39-55`, `:128-176`），而 WAL 主要负责输入消息的持久化、游标推进和故障重放（`crates/arkflow-core/src/wal/mod.rs:15-30`, `:235-270`）。消息元数据已经包含 partition、key、event timestamp 和 ingest timestamp（`crates/arkflow-core/src/lib.rs:58-68`, `:484-518`），但尚未形成可跨 Compute 节点恢复的状态计算模型。

数据平台团队需要的不只是更多连接器，而是能够运行有状态、事件时间驱动、可恢复且可扩展的流计算任务。现在建立统一的分布式任务、状态和 checkpoint 契约，可以把现有 Hub–Agent Fleet 基础演进为真正的流计算平台，并为后续 CDC、物化视图和实时查询服务留下稳定基础。

## What Changes

- 新增面向 SQL-first 流计算任务的 Job/DAG 执行模型，支持 task、subtask、partition 和 keyed state。
- 新增事件时间、watermark、乱序和迟到事件的统一运行时语义。
- 新增嵌入式热状态后端与对象存储 checkpoint/savepoint 契约，支持失败恢复和状态迁移。
- 新增任务级部署、恢复、重平衡、滚动升级和状态检查能力。
- 新增 SQL Job API，将 source、sink、窗口、分区键、状态和恢复策略表达为可部署任务。
- 扩展 Hub–Agent 控制面，使其能够管理 Job、Task、Checkpoint 和恢复状态，并保留现有 Stream/YAML 生命周期。
- **BREAKING** 新分布式引擎使用独立 Job API；现有 YAML 本地 Stream 保持兼容，但不自动转换为分布式 Job。

### Non-goals

- 本变更不实现完整 PostgreSQL-compatible streaming database 或物化视图查询服务。
- 本变更不实现多 Hub 共识、全球调度或跨集群联邦。
- 本变更不移除现有 YAML、本地 CLI、WAL 和连接器运行时。
- 本变更不以增加连接器数量或 AI 产品化作为主要目标。

## Capabilities

### New Capabilities

- `distributed-job-runtime`: 定义 Job DAG、task/subtask、partition 分配、Compute 执行和任务生命周期。
- `event-time-processing`: 定义事件时间、watermark、乱序、迟到事件和窗口触发语义。
- `keyed-state-backend`: 定义 keyed state、窗口/Join 状态、嵌入式本地 KV 和状态版本。
- `checkpoint-recovery`: 定义 checkpoint、savepoint、故障恢复、状态迁移和恢复可观测性。
- `streaming-job-api`: 定义 SQL-first Job 配置、验证、执行计划和 Rust 扩展边界。

### Modified Capabilities

- `control-plane-fleet`: Fleet 资源与能力发现扩展到 Job、Task、Checkpoint 和 Compute 任务执行状态。
- `control-plane-reconciliation`: desired/observed/convergence 模型扩展到 Job 部署、恢复、重平衡和 checkpoint 目标。

## Impact

- 主要影响 `arkflow-core` 的 Stream/Pipeline/metadata/runtime 抽象，以及新增状态、时间和任务调度模块。
- 影响 `arkflow-server` 的 Hub、Agent、持久化模型和控制面 API；需要保持现有 Node/Stream/Config 资源兼容。
- 影响 `arkflow-plugin` 的窗口、Join、SQL 和输入连接器，使其能够声明分区键、时间属性和状态需求。
- 需要新增对象存储 checkpoint 依赖或复用现有 WAL 对象存储基础，但 checkpoint 与输入 WAL 必须保持独立生命周期。
- 需要新增 SQL Job schema、运行时协议、状态/恢复指标以及单机、多节点和故障注入测试。
