---
sidebar_position: 20
title: HTTP API 参考
description: 节点 API 与 Hub 操作员/Agent API(/api/v1)的完整路由参考。
---

# HTTP API 参考

控制平面由一个按角色提供不同服务面的二进制对外暴露三类 API:

| API 面 | 基础路径 | 受众 |
|---------|-----------|----------|
| 节点 API | `/` | 单个 ArkFlow 节点:本地流生命周期、配置、事件、指标。 |
| Hub 操作员 API | `/api/v1` | 集群(fleet)操作员与 Web 控制台:作业、节点、灰度发布(rollout)、审计。 |
| Hub Agent API | `/api/v1/agent/*` | 计算节点(Agent):注册、心跳、报告、命令拉取。 |

健康、就绪与存活探针(路径可配置)以及 `GET
/metrics`(Prometheus 文本格式)同时在节点与 Hub 服务上提供。

Hub 操作员 API 是一个**期望状态(desired-state)API**:一次成功的 HTTP 变更只意味着 Intent 被持久化接受——并不代表某个节点已经执行了该命令,也不代表流已收敛(convergence)。请轮询资源或订阅事件来观察收敛。

## 约定 {#conventions}

### 资源模型

一条流有三个相互独立的视图:

```text
desired       operator target: state, generation, config_version
observed      latest node report: state, generation, config_version
convergence   comparison: unknown, pending, applying, in_sync, degraded, blocked
```

操作记录还会暴露 `intent_id`、`attempt_id`、代数(generation)、重试元数据、失败分类以及最近一次观测结果。

### 乐观并发与幂等性

规范的生命周期变更:

```http
PUT /api/v1/nodes/{node_id}/streams/{stream_id}/desired-state
Authorization: Bearer <operator-token>
Content-Type: application/json
If-Match: "generation-3"
Idempotency-Key: orders-desired-4
X-Correlation-ID: request-123

{"state":"running","config_version":"cfg-17"}
```

Hub 返回 `202 Accepted`,其中 `Location` 指向该操作,并带有新代数的 `ETag`:

```json
{
  "operation_id": "intent-4-17",
  "intent_id": "intent-4-17",
  "node_id": "node-a",
  "stream_id": "orders",
  "generation": 4,
  "desired_state": "running",
  "config_version": "cfg-17",
  "convergence": "pending"
}
```

- `If-Match` 是一个比较并交换(compare-and-swap)守卫:代数过旧时返回 `412` 与 `generation_conflict`。
- `Idempotency-Key` 为同一主体、同一资源、同一请求体的重试去重;同一 key 搭配不同请求体使用会返回 `409` 与 `idempotency_key_reused`。
- 节点离线**不会**拒绝期望状态写入;Intent 会在节点重新连接时派发。

### 分页

集合端点返回:

```json
{"items": [], "page": 1, "page_size": 50, "total": 0}
```

`page_size` 上限为 100。操作列表支持 `node_id`、`resource_id`、`operation`、`state` 与 `correlation_id` 过滤。请使用 `intent_id` 与 `generation` 作为稳定的对账依据,不要假设某个命令 ID 代表最终成功。

### 问题信封

错误使用稳定的 `code`、人类可读的 `message`、原样回传的 `correlation_id`,以及可选的机器可读 `details`:

```json
{
  "code": "generation_conflict",
  "message": "Expected generation 3, current generation 4",
  "correlation_id": "request-123",
  "details": {
    "expected_generation": 3,
    "current_generation": 4,
    "resource": {"node_id": "node-a", "stream_id": "orders"}
  }
}
```

## 节点 API

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/system` | 静态系统描述符(节点身份、能力)。 |
| `GET` | `/status` | 节点的运行时状态。 |
| `GET` | `/nodes` | 节点视图(单节点部署别名)。 |
| `GET` | `/node` | 节点视图(单数别名)。 |
| `GET` | `/streams` | 列出流及其观测状态。 |
| `GET` | `/streams/{id}` | 单条流的资源视图。 |
| `POST` | `/streams/{id}/start` | 启动一条流。 |
| `POST` | `/streams/{id}/stop` | 停止一条流。 |
| `POST` | `/streams/{id}/restart` | 重启一条流。 |
| `GET` | `/operations` | 列出操作(`node_id`、`resource_id`、`operation`、`state`、`correlation_id` 过滤)。 |
| `GET` | `/operations/{id}` | 单条操作记录。 |
| `DELETE` | `/operations/{id}` | 取消一个待处理的 Intent。 |
| `GET` | `/events` | 查询本地事件日志。 |
| `GET` | `/configuration` (`/config`) | 当前配置。 |
| `POST` | `/configuration/validate` (`/config/validate`) | 校验一个配置体。 |
| `GET`/`PUT` | `/configuration/draft` | 读取/保存工作草稿配置。 |
| `GET` | `/configuration/diff` | 对比草稿与生效配置。 |
| `GET` | `/configuration/versions` (`/config/versions`) | 配置版本历史。 |
| `POST` | `/configuration/apply` (`/config/apply`) | 应用一个配置版本。 |
| `POST` | `/configuration/rollback/{id}` (`/config/rollback/{id}`) | 回滚到先前的配置版本。 |
| `GET` | `/components` | 已注册组件。 |
| `GET` | `/components/{kind}/{name}` | 单个组件的 schema 与示例。 |
| `GET` | `/schema` | 引擎配置 JSON Schema。 |
| `GET` | `/metrics` | Prometheus 指标。 |

## Hub 操作员 API(`/api/v1`)

### 作业

作业与流使用相同的期望状态语义管理。所有变更都接受一份已校验的作业规格,并返回带有操作引用的 `202 Accepted`。

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `POST` | `/jobs` | 创建作业:`{"spec": ..., "desired_state": "stopped" \| "running"}`。 |
| `POST` | `/jobs/validate` | 在创建任何内容之前,针对目标 `node_ids` 深度校验规格。 |
| `GET` | `/jobs` | 列出作业。 |
| `GET` | `/jobs/{id}` (`/jobs/{id}/status`) | 作业的观测状态。 |
| `GET` | `/jobs/{id}/detail` | 分配与收敛详情。 |
| `GET` | `/jobs/{id}/versions` | 版本历史。 |
| `GET` | `/jobs/{id}/plan` | 解释计划:算子边界、分区路由、有状态算子、检查点策略。 |
| `PUT` | `/jobs/{id}/desired-state` | 按 `If-Match` / `Idempotency-Key` 契约启动/停止。 |
| `GET`/`POST` | `/jobs/{id}/checkpoints` | 列出恢复工件 / 触发一次屏障检查点。 |
| `GET`/`POST` | `/jobs/{id}/savepoints` | 列出工件 / 触发一次保存点(savepoint)。 |
| `POST` | `/jobs/{id}/upgrades` | 升级到新版本(作业必须已停止且已收敛)。 |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/rollback` | 回滚到兼容的保存点。 |
| `POST` | `/jobs/{id}/actions/{action}` | 一次性操作,生命周期语义与流操作等价。 |

推荐的提交流程:先 `validate`,以 `desired_state: "stopped"` 提交,检查 `detail` 与 `plan`,再切换到 `running`。恢复工件(recovery artifacts)绑定到作业版本与状态格式版本;不兼容的工件会在还原之前被拒绝,而不是破坏状态。

### 节点与流

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/system` | Hub 系统描述符。 |
| `GET` | `/nodes` | 集群节点注册表。 |
| `GET` | `/streams` | 整个集群中的流。 |
| `GET` | `/nodes/{node_id}/streams/{id}` | 某节点上某条流的权威时点视图。 |
| `PUT` | `/nodes/{node_id}/streams/{id}/desired-state` | 规范的生命周期变更(参见[约定](#conventions))。 |
| `POST` | `/nodes/{node_id}/streams/{id}/{action}` | 定向的一次性命令(`start`、`stop` 等)。 |
| `POST` | `/nodes/{node_id}/streams/{id}/actions/restart` | 重启操作;只有当 Agent 上报匹配的 `action_id` 后才算收敛。 |
| `GET` | `/nodes/{node_id}/configuration` | 节点配置视图。 |
| `GET` | `/nodes/{node_id}/configuration/versions` | 节点配置历史。 |
| `POST` | `/nodes/{node_id}/configuration/apply` | 向单个节点应用配置。 |
| `POST` | `/nodes/{node_id}/configuration/rollback/{version}` | 回滚节点配置。 |
| `POST` | `/nodes/{node_id}/drain` | 排空节点(维护前迁走其分配)。 |
| `POST`/`DELETE` | `/nodes/{node_id}/maintenance` | 进入/退出维护模式。 |

旧的不定向节点的流变更路由仍作为适配器保留,它们创建的是同一条持久化 Intent 管道。

### 操作、事件、审计

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/operations` | 全集群范围列出操作。 |
| `GET` | `/operations/{id}` | 单条操作记录。 |
| `DELETE` | `/operations/{id}` | 取消一个待处理的 Intent(绝不会撤销已执行的副作用)。 |
| `GET` | `/operations/status` | 面向仪表盘的运维状态摘要。 |
| `GET` | `/events` | 查询集群事件日志。 |
| `GET` | `/events/stream` | 实时事件的 Server-Sent Events 流。 |
| `GET` | `/audit` | 有界审计历史(`?resource_id={id}`),包含被接受与被拒绝的变更。 |

`DELETE /operations/{intent_id}` 取消的是 Intent,而不是已经执行的副作用:派发之前会抑制待处理的工作;派发之后,Attempt 的结果仍然可见。

审计记录包含操作者、目标资源、节点、关联 ID、结果(`accepted`/`rejected`)以及稳定的失败码(`node_unavailable`、`capacity`、`incompatible_capability`、`expired`)。消息只包含标量操作元数据——绝不含凭据或作业配置体。

### 灰度发布(Rollouts)

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET`/`POST` | `/rollouts` | 列出灰度发布 / 创建灰度发布计划。 |
| `GET` | `/rollouts/{id}` | 灰度发布状态与各节点进度。 |
| `POST` | `/rollouts/{id}/actions` | 灰度发布操作:暂停、恢复、取消、回滚。 |

这些路由背后的状态机参见[对账式灰度发布与恢复](/zh-Hans/docs/operate/control-plane/reconciliation)。

### 发现与指标

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/components` | 集群镜像中已注册的组件。 |
| `GET` | `/components/{kind}/{name}` | 单个组件的 schema 与示例。 |
| `GET` | `/schema` | 引擎配置 JSON Schema。 |
| `GET` | `/metrics` | Prometheus 文本格式指标。 |

除就绪度、对账、outbox 与集群状态仪表之外,命令派发还暴露 `arkflow_command_duration_bucket{command,le}`(从入队到确认的延迟,附带 `_count`/`_sum`)与 `arkflow_command_total{command,outcome}`。标签来自固定词表——命令类型(`job_start`、`job_stop`、`job_checkpoint`、`job_savepoint`、流操作)与结果类别(`enqueued`、`acknowledged`、`succeeded`、`failed`、`timed_out`、`node_unavailable`、`capacity`、`rejected`);未知命令名会折叠为 `other`,资源 ID、关联 ID 与错误文本绝不会成为标签。计数器在 Hub 重启时重置。

## Hub Agent API(`/api/v1/agent/*`)

Agent 契约与操作员契约相互独立:Agent 使用节点会话凭据认证,拉取命令并推送观测。

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `POST` | `/agent/register` | 注册节点;建立会话。负载声明节点 `capabilities`(例如 `network_shuffle`),并在数据面(data plane)启用时声明对等端用于远程边的可路由 `data_address`。 |
| `POST` | `/agent/heartbeat` | 附带租约续期的心跳;重新确认能力与数据地址。 |
| `POST` | `/agent/report` | 观测到的流/节点状态;携带 `boot_id` 与单调递增的 `report_seq`。 |
| `POST` | `/agent/job-observations` | 上报来自同置内核运行时的作业级观测。 |
| `GET` | `/agent/commands` | 拉取待处理命令。 |
| `POST` | `/agent/commands/{id}/result` | 上报命令结果。 |

命令携带代数、Attempt ID、配置版本与过期时间。`split` 放置的 `job_start` 还会携带完整的 `task_nodes` 映射与 `node_data_ports`,因此每个节点无需额外查询即可推导出自己的远程边;Hub 只会把这类放置派发给声明了 `network_shuffle` 能力与数据地址的节点。命令确认只是传输层状态——收敛始终由报告推导,绝不来自确认(ack)。

## 相关页面

- [控制平面概览](/zh-Hans/docs/operate/control-plane/overview) — 这些路由背后的架构。
- [控制平面运维](/zh-Hans/docs/operate/control-plane/operations) — 使用该 API 的操作员工作流。
- [CLI 参考](./cli.md) — 节点 API 的本地单二进制替代方案。
