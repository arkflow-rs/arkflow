# Proposal: add-data-plane-observability

## Why

v1 统一内核合入 main 后，所有 YAML `jobs` 与 Agent Job 都跑在统一执行内核上，内核已经产出结构化指标 `KernelMetricsSnapshot`（`crates/arkflow-core/src/executor/metrics.rs:113`：per-chain batches/rows/errors/in-flight/延迟 + checkpoint 时长/失败 + watermark lag + 迟到事件），但这些数据面指标没有任何 Prometheus 出口：

1. 本地 `/metrics` 只导出 legacy Stream 指标（`crates/arkflow-server/src/lib.rs:3123`），手拼文本、无 HELP/TYPE 与类型声明，内核 Job 指标序列完全缺失；
2. Agent 心跳已上报 Job/内核指标（`crates/arkflow-server/src/agent.rs:344`、`agent.rs:1671`），Hub 也已聚合（`crates/arkflow-server/src/hub.rs:4511`），但 Hub `/metrics` 只导出控制面序列（`crates/arkflow-server/src/lib.rs:2333`），数据面指标滞留在 JSON 诊断端点（`lib.rs:899`、`lib.rs:2404`），无法被 Prometheus 抓取；
3. `serve()` 在 `server.enabled=false` 时直接返回（`crates/arkflow-server/src/lib.rs:258`），不开控制面 API 的纯数据面部署没有任何 `/metrics` 与 readiness 端点——这是 issue #768「单进程 /ready」的诉求；现有 `/readiness`（`lib.rs:3179`）仅反映控制面状态且随 API server 一起消失。

`openspec/PLANNING.md` 第七节已将「数据面可观测性」列为 v1 合入后的第一优先方向：`prometheus` crate 已在 workspace（`Cargo.toml:35`），缺的只是导出层。

## What Changes

- 新增进程级 observability 导出层：即使控制面 API server 被禁用，进程仍以最小 HTTP 面提供 `/metrics`、`/ready`、`/live`（地址与路径可配置），不暴露任何资源 API。
- 本地 `/metrics` 增加统一内核 Job 指标序列（per-chain、checkpoint、watermark lag、late events），与现有 legacy Stream 序列并存；全部序列带 HELP/TYPE 行并正确声明 counter/gauge 类型。
- Hub `/metrics` 导出 Agent 心跳上报的数据面指标，标签限定为有界词表（node、job/stream 标识）；消息内容、错误消息、关联 ID 不得进入标签。
- 固化数据面指标的命名、类型与标签契约（哪些序列、counter 还是 gauge、重启归零语义）。
- 现有 legacy Stream 指标序列、`/readiness`、`/liveness`、`/health` 的路径与语义保持不变（兼容）。

## Capabilities

### New Capabilities

- `data-plane-observability`: 数据面（legacy Stream 与统一内核 Job）的 Prometheus 导出与进程健康端点契约——导出序列与标签词表、HELP/TYPE 与 counter/gauge 规范、API server 禁用时仍可用的独立 observability 监听、Hub 对 Agent 上报指标的有界聚合导出、低基数约束。

### Modified Capabilities

- 无。`stream-runtime-control` 的 Stream 指标要求与 `control-plane-observability` 的 Hub 控制面指标要求保持不变；新增序列与端点行为由新 capability 承载。

## Impact

- **代码**：
  - `crates/arkflow-server/src/lib.rs`（`/metrics` handler 扩展、observability 独立监听与 `serve` 逻辑）
  - `crates/arkflow-server/src/hub.rs`（Agent 上报数据面指标的聚合导出口）
  - 可能新增 `crates/arkflow-server/src/metrics.rs`（registry/编码，收敛手拼文本）
  - `crates/arkflow/src/main.rs`（observability 任务随 Engine 启动）
  - `crates/arkflow-core`：读取 Job 运行指标的现有接口（`KernelMetrics::snapshot`）已足够，预期仅小改动
- **依赖**：`prometheus = "0.13"` 已在 workspace（`Cargo.toml:35`），无新增外部依赖
- **配置**：server/observability 相关 YAML 字段（地址、路径、开关）需写入配置参考文档
- **测试**：Prometheus 文本格式断言（HELP/TYPE/序列）、server 禁用时的端点可用性、Hub 聚合导出的标签有界性

## Non-goals

- OpenTelemetry trace 导出（另立 change）。
- 新增采集点或指标扩展（如 per-plugin WAL 指标、延迟直方图分桶）：本 change 只导出 `KernelMetrics`、Stream `RuntimeMetrics` 与 Agent 心跳已产出的数据。
- Console 指标可视化。
- 指标远程写入（push gateway/remote write）与 Hub 高可用。
