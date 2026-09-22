---
sidebar_position: 6
title: 可观测性
description: 在生产环境运行 ArkFlow 所需的指标、事件、日志与仪表盘。
---

# 可观测性

ArkFlow 可通过三个通道观测:**Prometheus 指标**、结构化**事件日志**(附带实时 SSE 流)以及
**tracing 日志**。三者都能在单节点上使用,配合控制平面还可以跨机群聚合。

## 指标

引擎节点与 Hub 都在 `GET /metrics` 暴露 Prometheus 文本格式指标。可使用任何 Prometheus
兼容的采集器抓取:

```yaml validate=foreign reason="Prometheus scrape config"
scrape_configs:
  - job_name: arkflow
    static_configs:
      # Engine nodes: dedicated observability listener (default 127.0.0.1:8081).
      # Hub: the control-plane API server (default 127.0.0.1:8080).
      - targets: ["node-a.example:8081", "hub.example:8080"]
```

### 数据平面指标

每个进程都会为它所运行的数据平面导出指标:旧版 Stream 位于 `arkflow_stream_*`,统一内核
Job 位于 `arkflow_job_*`。所有序列都携带 `# HELP`/`# TYPE` 元数据;单调递增的计数器类型为
`counter` 并以 `_total` 后缀命名,积压与延迟序列则是 `gauges`。

| 指标 | 类型 | 标签 | 含义 |
|--------|------|--------|---------|
| `arkflow_job_chain_batches_in_total` / `_batches_out_total` | counter | `job`, `chain` | 每条链路接收/发出的批次数。 |
| `arkflow_job_chain_rows_total` | counter | `job`, `chain` | 流经链路的行数。 |
| `arkflow_job_chain_errors_total` | counter | `job`, `chain` | 处理错误。 |
| `arkflow_job_chain_in_flight` | gauge | `job`, `chain` | 停留在链路出边上的信封(积压)。 |
| `arkflow_job_chain_mean_latency_us` | gauge | `job`, `chain` | 平均每批处理时间(µs)。 |
| `arkflow_job_checkpoint_duration_ms` | gauge | `job` | 最近一次检查点耗时。 |
| `arkflow_job_checkpoint_failures_total` | counter | `job` | 失败的检查点。 |
| `arkflow_job_watermark_lag_ms` | gauge | `job` | 各源中最大的水位线滞后。 |
| `arkflow_job_late_events_total` | counter | `job` | 被丢弃/路由/更新的迟到事件。 |
| `arkflow_stream_input_messages` / `_output_messages` / `_restarts` / `_checkpoint_failures` / `_late_events` | counter | `stream_id` | 旧版 Stream 计数器(名称不变)。 |
| `arkflow_stream_in_flight` / `_mean_latency_us` / `_checkpoint_duration_ms` / `_watermark_lag_ms` | gauge | `stream_id` | 旧版 Stream gauge 指标。 |

标签使用固定词表——作业/链路/Stream 标识符,外加可选的 `node` 标签。消息内容、关联 ID
与错误文本绝不会成为标签,因此基数受限于所配置的负载规模(每个进程 O(作业数 × 链路数))。
进程重启时计数器会归零;请在稳定窗口上使用 `rate()`/`increase()`。

### 就绪与存活

每个进程都暴露 `GET /ready` 与 `GET /live`(路径可在 `health_check.observability` 下配置):

- `/ready` 在引擎完成启动所有已配置的 Stream 与 Job 之后返回成功;启动期间——或启动失败后——它会以稳定的
  `not_ready` 状态响应 `503`。
- `/live` 在进程运行期间返回成功,不依赖任何外部系统。

启用控制平面 API 服务时,它会在自己的地址上提供相同的端点;为了兼容,旧版 `/health`、`/readiness`
与 `/liveness` 路径保留原有语义。禁用 API 服务时,一个专用监听器(默认 `127.0.0.1:8081`)会继续提供
`/metrics`、`/ready` 与 `/live`——因此纯数据平面部署仍然可以被抓取和探测。参见
[`health_check.observability`](/zh-Hans/docs/reference/configuration#health_checkobservability)。

### Hub 机群导出

Hub 会在自己的 `/metrics` 端点上重新导出其 Agent 上报的数据平面指标,并附加一个 `node`
标签以区分上报者。Job/Stream 序列只为持有有效租约的 Agent 导出——停止上报的 Agent(租约过期或已注销)会从暴露中消失;
原始的 `arkflow_node_metric` 序列则保留最后一次上报的值。配置了运维令牌时,该端点要求携带它
(`Authorization: Bearer <operator token>`);未配置令牌时,该端点与 Hub API 的其余部分一样无需认证即可访问。

Hub 侧的指标族:

| 指标 | 含义 |
|--------|---------|
| `arkflow_control_plane_ready` | Hub 完成恢复并接受写入时为 1。 |
| `arkflow_command_duration_bucket{command,le}` / `_count` / `_sum` | 每类命令从入队到确认的延迟。 |
| `arkflow_command_total{command,outcome}` | 分发计数器。结果类别包括 `succeeded`、`failed`、`timed_out`、`node_unavailable`、`capacity`、`rejected`、`cancelled`、`superseded`、`acknowledged` 与 `expired`。 |
| `arkflow_reconciliation_runs_total` / `_failures_total` | 调和循环的活动与失败次数。 |
| `arkflow_outbox_pending` / `arkflow_outbox_claimed` | 命令 outbox 深度。 |
| `arkflow_stale_nodes` / `arkflow_active_attempts` / `arkflow_non_terminal_intents` | 机群收敛态势。 |
| `arkflow_nodes_state{state}` / `arkflow_nodes_maintenance_state{state}` | 按生命周期/维护状态统计的节点数。 |
| `arkflow_intents_state{state}` / `arkflow_attempts_state{state}` / `arkflow_rollouts_state{state}` | 按状态统计的 Intent/Attempt/灰度发布数量。 |
| `arkflow_node_compatibility{...}` / `arkflow_node_capability{...}` | 每节点的协议兼容性与声明的能力(如 `network_shuffle`)。 |
| `arkflow_node_metric{node_id,metric}` | 每个节点上报的最近一个流/处理指标值。 |
| `arkflow_job_*{node,job,chain}` | 上述数据平面指标,按上报节点区分。 |

## 事件

每一次被接受或拒绝的变更、节点状态转换与收敛变化都会记录到事件日志中:

```bash
# 查询机群事件日志
curl -H "Authorization: Bearer $TOKEN" \
  "https://hub.example/api/v1/events?node_id=node-a&page_size=50"

# 或者通过 SSE 订阅实时事件
curl -N -H "Authorization: Bearer $TOKEN" \
  "https://hub.example/api/v1/events/stream"
```

单节点会在本地 `GET /events` 暴露同一通道。控制台的[事件视图](./control-plane/console.md)
同时呈现查询与实时流。

## 日志

引擎通过 `tracing` 记录日志。级别与输出是配置项(不是 CLI 标志):

```yaml validate=fragment wrap=engine
logging:
  level: info        # trace | debug | info | warn | error
  format: json       # json | plain
  file_path: /var/log/arkflow/arkflow.log   # omit for stdout
```

`json` 格式适合日志采集器(Vector、Fluent Bit、Loki);`plain` 适合交互式调试。
当 `file_path` 无法打开时,引擎会退回 stdout 日志,并在 stderr 上说明。

## 追踪

在 `health_check.observability.tracing` 下启用 OTel trace 导出。span 经
OTLP HTTP-JSON 批量导出;默认关闭该节,对行为没有任何影响。

```yaml validate=fragment wrap=engine
health_check:
  observability:
    tracing:
      enabled: true
      endpoint: "http://localhost:4318/v1/traces"
      service_name: "arkflow"
```

v1 的 span 模型是每次 Job 执行的生命周期骨架:

- `job.run` —— 一次图执行的根 span,带 `chains` 属性(链数)。
- `chain.run` —— 每条链一个子 span(属性 `task`,链的入口任务 id),
  覆盖从启动到资源关闭的全程。

已知的 v1 边界:没有 per-batch 或 per-connector span(worker 池的上下文
传播是后续工作),跨节点不传播 trace 上下文——每个进程为其运行的 Job
各自作为 trace 根。导出器故障绝不影响数据面。

## 面向仪表盘的运维状态

`GET /api/v1/operations/status` 返回一个为状态页与告警设计的有界摘要:分发健康状况、待处理操作与机群收敛情况。
应基于这些字段告警,而不是抓取原始操作列表。

## 建议的告警

- 节点心跳/租约丢失(节点离线或被网络分区)。
- `arkflow_command_total{outcome="timed_out"|"failed"}` 持续增长。
- 流的收敛长时间停留在 `pending`/`applying`,超出灰度发布预算,或出现 `degraded`/`blocked`。
- Hub 就绪探测失败(存储/恢复受损)。
- `arkflow_job_chain_errors_total` 增长、`arkflow_job_checkpoint_failures_total`
  增长,或 `arkflow_job_watermark_lag_ms` 呈上升趋势。

## 相关页面

- [HTTP API 参考](/zh-Hans/docs/reference/api) —— 所有指标/事件路由。
- [恢复](./recovery.md) —— 这些信号触发时该做什么。
