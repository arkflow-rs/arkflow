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

### 主体回显 {#principal-echo}

Hub 面与本地平面的 `GET /system` 均包含可选的 `principal` 对象,描述当前请求已认证的主体:

```json
{
  "id": "ops",
  "roles": ["operator"],
  "actions": ["read", "operate", "configure", "manage_nodes", "manage_rollouts", "manage_webhooks"]
}
```

`actions` 由与变更授权相同的判定推导——回显不可能声称一个授权检查会拒绝的动作。凭证无效的请求会在回显任何身份之前被 401 拒绝;`principal: null` 只是在无法解析主体时仍产出响应的防御性形状。本地 standalone 平面恒回显其唯一的全权操作者 `operator (admin)`。该字段为增量添加:不认识它的消费方可以无行为变化地忽略它。

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

分页的集合端点(`/nodes`、`/streams`、`/operations`、`/events`、`/audit`)返回:

```json
{"items": [], "page": 1, "page_size": 50, "total": 0}
```

`page_size` 上限为 100。其余集合端点(`/jobs`、`/rollouts`、`/config-approvals`)返回纯数组,`GET /webhooks` 则把列表包在 `{"subscriptions": [...]}` 中。操作列表支持 `node_id`、`resource_id`、`operation`、`state` 与 `correlation_id` 过滤。请使用 `intent_id` 与 `generation` 作为稳定的对账依据,不要假设某个命令 ID 代表最终成功。

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

### 限速

两个控制面都对版本化 API 施加按凭证的请求限速;健康探针、`GET /metrics` 与 OIDC 登录流豁免。桶以凭证的截断哈希为键——静态/结构化 operator 凭证按凭证分桶,OIDC 浏览器会话按会话分桶,agent 请求按节点分桶(`Authorization: Bearer` 头与旧通道 `session_token` 查询参数归入同一桶)。超出桶容量的请求在进入任何 handler 之前被拒绝:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json

{"code":"rate_limited","message":"Control-plane request rate limit exceeded; retry after the indicated interval"}
```

`Retry-After` 携带整秒补充估算。被拒请求不产生审计事件、不产生操作记录,并经 `arkflow_control_plane_rate_limit_rejections_total{plane,key_kind}` counter family 导出(`plane` 为 `local` 或 `hub`;`key_kind` 为 `principal`、`node` 或 `catch_all`——吸收超出有界桶表流量的共享桶)。配置位于节点面的 `health_check.rate_limit`(control-API 节以 flatten 形态并入 `health_check`)与 Hub 侧的 `ARKFLOW_RATE_LIMIT_*` 环境变量(见[部署](../operate/control-plane/deploy.md#限速))。默认开启且限值远高于合法流量;`enabled: false` 精确恢复不限速行为。

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
| `GET` | `/jobs/{id}/upgrades/{upgrade_id}` | 升级状态:阶段、保存点、超时、最近错误。 |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/actions` | 升级操作:`pause`、`resume`、`cancel`、`rollback`。 |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/rollback` | 回滚到兼容的保存点。 |
| `POST` | `/jobs/{id}/actions/{action}` | 一次性操作,生命周期语义与流操作等价。 |

推荐的提交流程:先 `validate`,以 `desired_state: "stopped"` 提交,检查 `detail` 与 `plan`,再切换到 `running`。恢复工件(recovery artifacts)绑定到作业版本与状态格式版本;不兼容的工件会在还原之前被拒绝,而不是破坏状态。

### 节点与流

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/system` | Hub 系统描述符,含 `principal: {id, roles, actions}` 回显当前已认证调用方的身份与被授权动作(增量字段;见[主体回显](#principal-echo))。 |
| `GET` | `/status` | 机群聚合的引擎状态:streams total/running/failed 对全部注册节点求和。 |
| `GET` | `/nodes` | 集群节点注册表。支持标签过滤:`?label=env%3Dprod%2Ctier%3Dedge`(逗号连接的 `key=value` 项,AND 语义)。 |
| `GET` | `/streams` | 整个集群中的流。 |
| `GET` | `/nodes/{node_id}/streams/{id}` | 某节点上某条流的权威时点视图。 |
| `PUT` | `/nodes/{node_id}/streams/{id}/desired-state` | 规范的生命周期变更(参见[约定](#conventions))。 |
| `POST` | `/nodes/{node_id}/streams/{id}/{action}` | 定向的一次性命令(`start`、`stop` 等)。 |
| `POST` | `/nodes/{node_id}/streams/{id}/actions/restart` | 重启操作;只有当 Agent 上报匹配的 `action_id` 后才算收敛。 |
| `GET` | `/nodes/{node_id}/configuration` | 节点配置视图。 |
| `GET` | `/nodes/{node_id}/configuration/versions` | 节点配置历史。 |
| `POST` | `/nodes/{node_id}/configuration/validate` | 在应用前将配置候选对单个节点校验。 |
| `GET` | `/nodes/{node_id}/configuration/diff` | 对节点上两个配置版本求差异(`from`/`to` 查询参数)。 |
| `POST` | `/nodes/{node_id}/configuration/apply` | 向单个节点应用配置。 |
| `POST` | `/nodes/{node_id}/configuration/rollback/{version}` | 回滚节点配置。 |
| `POST` | `/nodes/{node_id}/drain` | 排空节点(维护前迁走其分配)。 |
| `POST`/`DELETE` | `/nodes/{node_id}/maintenance` | 进入/退出维护模式。 |
| `PUT` | `/nodes/{node_id}/labels` | 整体替换该节点的标签映射(参见[节点标签](#节点标签))。 |

#### 节点标签

`PUT /nodes/{node_id}/labels` 接收 `{"labels": {"env": "prod", "tier": "edge"}}`,语义为整体替换:提交的映射即该节点的完整标签集。需要 `ManageNodes` 动作(operator 与 admin 角色);接受的替换与拒绝均落审计(`node.labels`)。

- 标签由 Hub 管理:Agent 从不上报标签,Agent 重注册或 Hub 重启都不会改变标签。
- 边界:每节点最多 16 条;key 与 value 各 1-63 字符,以字母数字开头和结尾,中间允许 `-`、`_`、`.`。违规以 `invalid_labels` 拒绝,不会部分应用。
- Rollout 目标选择:`POST /rollouts`(以及审批提交)接受 `target_selector`——等值映射,与 `node_ids` 互斥。Hub 在**创建时一次性**解析为显式节点集(审批策略开启时在提交时解析);解析结果为空集以 `no_matching_nodes` 拒绝。之后的标签变更不会改变已创建 rollout 或待审批请求的目标。匹配读取持久化节点记录（`cp_nodes`）而非实时注册表:Hub 重启后尚未重注册的节点同样会被解析到;不可达节点保持可选——由 rollout 健康门控处理投递。

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
| `GET`/`PUT` | `/webhooks` | 列出/全量替换 webhook 告警订阅集(参见[Webhook 告警](#webhook-告警))。 |
| `POST` | `/webhooks/{id}/test` | 同步投递一条合成 `webhook.test` 测试事件。 |

#### Webhook 告警

`GET/PUT /webhooks` 以全量替换语义管理 webhook 订阅集(至多 16 条)。
两个路由(以及 `POST /webhooks/{id}/test`)都要求 `ManageWebhooks` 动作
——admin 与 operator 角色——每次接受的替换或拒绝均入审计
(`webhook.replace`)。每个条目包含 `id`、`url`(仅 `http`/`https`)、
`event_types`(前缀过滤)、`enabled` 与可选的只写 `secret`:响应永不回显
secret;省略时保留该 id 已存的密钥,显式空字符串则清除。

投递为 best-effort(仅 leader 转发、有界丢最旧队列、3 次退避重试、连续
5 次失败后自动暂停并发布 `webhook.paused`/`webhook.resumed` 生命周期事
件)。带签名的投递携带 `X-Arkflow-Signature`(请求体的十六进制
HMAC-SHA256)。完整契约参见
[Webhook 告警](/docs/operate/control-plane/webhook-alerting)。

`DELETE /operations/{intent_id}` 取消的是 Intent,而不是已经执行的副作用:派发之前会抑制待处理的工作;派发之后,Attempt 的结果仍然可见。

审计记录包含操作者、目标资源、节点、关联 ID、结果(`accepted`/`rejected`)以及稳定的失败码(`node_unavailable`、`capacity`、`incompatible_capability`、`expired`)。消息只包含标量操作元数据——绝不含凭据或作业配置体。

### 灰度发布(Rollouts)

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET`/`POST` | `/rollouts` | 列出 rollout / 创建 rollout 计划。目标选择为 `node_ids` **或** `target_selector`(标签等值映射,创建时一次性解析;参见[节点标签](#节点标签))。 |
| `GET` | `/rollouts/{id}` | 灰度发布状态与各节点进度。 |
| `POST` | `/rollouts/{id}/actions` | 灰度发布操作:暂停、恢复、取消、回滚。 |

当 Hub 启用 `control_api.approval_required` 后,创建类路由(`POST /rollouts`
与节点级 apply/rollback 路由)会以 `approval_required` 错误拒绝直接创建,
发布改走审批请求:

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET`/`POST` | `/config-approvals` | 列出审批请求(`?status=pending\|approving\|approved\|rejected\|expired`)/ 提交一个钉住版本、目标节点与批大小的请求。与 `POST /rollouts` 一样接受 `target_selector`;请求在提交时把选择器解析为显式节点集并钉住。 |
| `GET` | `/config-approvals/{id}` | 单个审批请求及其钉住的快照。 |
| `POST` | `/config-approvals/{id}/decide` | 批准或拒绝(`{"decision":"approve"\|"reject","note":"..."}`)。需要 `ApproveRollouts` 权限(仅 admin 角色),且审批人必须不同于提交人。 |

批准后由系统创建 rollout 并以审批人作为 actor;回滚只有在目标版本
曾应用到受影响节点时才豁免审批。

这些路由背后的状态机参见[对账式灰度发布与恢复](/zh-Hans/docs/operate/control-plane/reconciliation)。

### 登录会话(OIDC)

当 Hub 配置了 OIDC 提供方时(参见[部署](/zh-Hans/docs/operate/control-plane/deploy#oidc-jwt-联邦)),操作员 API 还会提供一个浏览器登录流:

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/auth/oidc/status` | 公开探测端点,报告 `login_enabled`、`authenticated` 与当前主体。 |
| `GET` | `/auth/oidc/login` | 302 跳转到身份提供方(PKCE S256 与 `nonce` 绑定到 HttpOnly cookie)。 |
| `GET` | `/auth/oidc/callback` | 用 code 换取 id_token 并创建服务端会话。 |
| `GET` | `/auth/oidc/logout` | 删除服务端会话并清除 cookie。 |

### 发现与指标

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/components` | 集群镜像中已注册的组件。 |
| `GET` | `/components/{kind}/{name}` | 单个组件的 schema 与示例。 |
| `GET` | `/schema` | 引擎配置 JSON Schema。 |
| `GET` | `/metrics` | 默认 Prometheus 文本格式;`?format=json`(或带 `Accept: application/json` 的请求)返回 `{"items": ..., "aggregate": ...}`——各节点指标加机群聚合,可用 `?node_id=` 收窄。 |

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
