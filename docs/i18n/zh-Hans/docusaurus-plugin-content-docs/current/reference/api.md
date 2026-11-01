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

`actions` 由与变更授权相同的判定推导——回显不可能声称一个授权检查会拒绝的动作。凭证无效的请求会在回显任何身份之前被 401 拒绝;`principal: null` 只是在无法解析主体时仍产出响应的防御性形状。本地 standalone 平面恒回显其唯一的全权操作者 `operator (admin)`。该字段为增量添加:不认识它的消费方可以无行为变化地忽略它。`manage_users` 动作只对 `admin` 主体出现,用于门控[用户与凭证管理](#users-and-credentials)路由。

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

`page_size` 上限为 100。其余集合端点(`/jobs`、`/rollouts`、`/config-approvals`、`/users` 与 `/users/{id}/credentials`)返回纯数组,`GET /webhooks` 则把列表包在 `{"subscriptions": [...]}` 中。操作列表支持 `node_id`、`resource_id`、`operation`、`state` 与 `correlation_id` 过滤。请使用 `intent_id` 与 `generation` 作为稳定的对账依据,不要假设某个命令 ID 代表最终成功。

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

作业与流使用相同的期望状态语义管理。变更返回更新后的作业记录:创建与暂停/恢复返回 `202 Accepted`,其余生命周期变更返回 `200 OK`。作业生命周期变更没有独立的 operation 记录——观察返回的作业及其 `convergence` 即可。

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `POST` | `/jobs` | 创建作业:`{"spec": ..., "desired_state": "stopped" \| "running"}`。创建**不接受** `paused`。 |
| `POST` | `/jobs/validate` | 在创建任何内容之前,针对目标 `node_ids` 深度校验规格。 |
| `GET` | `/jobs` | 列出作业。 |
| `GET` | `/jobs/{id}` (`/jobs/{id}/status`) | 作业的观测状态。 |
| `GET` | `/jobs/{id}/detail` | 分配与收敛详情。 |
| `GET` | `/jobs/{id}/versions` | 版本历史。 |
| `GET` | `/jobs/{id}/plan` | 解释计划:算子边界、分区路由、有状态算子、检查点策略。 |
| `PUT` | `/jobs/{id}/desired-state` | 生命周期变更:`{"state": "running" \| "stopped" \| "paused"}`。内部执行世代比较并交换;未实现 `If-Match` / `Idempotency-Key` 请求头(见[作业暂停与恢复](#job-pause-and-resume))。 |
| `GET`/`POST` | `/jobs/{id}/checkpoints` | 列出恢复工件 / 触发一次屏障检查点。 |
| `GET`/`POST` | `/jobs/{id}/savepoints` | 列出工件 / 触发一次保存点(savepoint)。 |
| `POST` | `/jobs/{id}/actions/pause` | 暂停一个已收敛的运行中作业:先落恢复工件,再停机。`202` + 作业记录。 |
| `POST` | `/jobs/{id}/actions/resume` | 从暂停工件恢复暂停中的作业(提升世代 + 恢复启动)。`202` + 作业记录。 |
| `POST` | `/jobs/{id}/upgrades` | 升级到新版本(作业必须已停止且已收敛)。 |
| `GET` | `/jobs/{id}/upgrades/{upgrade_id}` | 升级状态:阶段、保存点、超时、最近错误。 |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/actions` | 升级操作:`pause`、`resume`、`cancel`、`rollback`。 |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/rollback` | 回滚到兼容的保存点。 |
| `POST` | `/jobs/{id}/actions/{action}` | 一次性操作:`start`、`stop`、`restart`、`pause` 或 `resume`。`restart` 是提升世代并从恢复工件还原——不是流式重启。在暂停中的作业上,`start`/`restart` 等价于 resume,`stop` 清除暂停记账。 |

推荐的提交流程:先 `validate`,以 `desired_state: "stopped"` 提交,检查 `detail` 与 `plan`,再切换到 `running`。恢复工件(recovery artifacts)绑定到作业版本与状态格式版本;不兼容的工件会在还原之前被拒绝,而不是破坏状态。

#### 作业暂停与恢复 {#job-pause-and-resume}

作业暂停是一次**持久静默**:Hub 先在暂停世代落恢复工件(`recovery: latest_checkpoint` 落检查点,`latest_savepoint` 落保存点;规格中没有 `checkpoint` 配置的作业直接停机),然后停止作业。变更返回 `202` 与作业记录,并且在所有目标分配都被观测为 `stopped` 之前,作业不会被报告为已暂停。

```http
POST /api/v1/jobs/{job_id}/actions/pause
POST /api/v1/jobs/{job_id}/actions/resume

PUT /api/v1/jobs/{job_id}/desired-state
{"state": "paused"}
```

`desired_state` 有三个取值——`running`、`stopped` 与 `paused`——但创建作业时仍只接受 `running` 与 `stopped`。在暂停中的作业上,`{"state": "running"}` 等价于 resume,`{"state": "stopped"}` 会停止它并清除暂停记账。desired-state 路由内部执行世代比较并交换(CAS),并返回更新后的作业记录(`200`);与上文流式 desired-state 路由不同,它**没有**实现 `If-Match` / `Idempotency-Key` 请求头。

作业响应是增量式的:从未暂停过的作业不携带下列任何字段。`GET /jobs` 与 `GET /jobs/{id}` 返回作业记录;`GET /jobs/{id}/detail` 把它嵌在 `job` 下。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `pause_phase` | `capturing` \| `stopping` \| `paused` \| `resuming` | 持久化的暂停状态机相位;作业未暂停时缺失。 |
| `pause_artifact_id` | string | 暂停世代上落下的恢复工件。 |
| `pause_prev_checkpoint_id` | string | 暂停落工件前的恢复指针;落工件中止时会被还原。 |
| `pause_retry_count` | integer | 有界的落工件重派计数(无重派时为 `0`)。 |
| `paused_at_ms` | integer | 暂停请求发起时间。 |
| `paused_by` | string | 发起暂停的主体 id。 |

暂停作业的收敛由相位定义:只有当 `pause_phase` 为 `paused` 且所有目标分配都被观测为 `stopped` 时才收敛;`capturing`、`stopping` 与 `resuming` 报告 `reconciling`,离线节点会让暂停保持 reconciling 而不是误报收敛。resume 提升世代、保持恢复指针指向暂停工件,并走普通恢复启动;工件屏障之后、teardown 完成之前处理的事件会重放(事务型 sink 精确一次,非事务型 sink 至少一次),没有 checkpoint 配置的作业则冷启动。暂停相位存在时原子模式与停止模式升级都会被拒绝,升级编排拥有作业期间暂停/恢复也会被拒绝(见[原子作业升级](/zh-Hans/docs/operate/control-plane/job-upgrades#pause-orchestration-vs-job))。

| 状态码 | 错误码 | 触发条件 |
| --- | --- | --- |
| `409` | `job_not_running` | 暂停要求期望态与观测态均为 `running` 且作业已收敛。 |
| `409` | `pause_in_progress` | 暂停相位已在途,或 resume 在相位落定为 `paused` 之前到达。 |
| `409` | `job_already_paused` | 对已处于 `paused` 相位的作业再次暂停。 |
| `409` | `pause_not_paused` | 没有活动暂停相位时 resume(或经由暂停状态机的 stop)。 |
| `409` | `job_paused` | 因暂停相位存在而拒绝升级。 |
| `409` | `pause_requires_recovery_policy` | `recovery: fail` 永远无法选出恢复工件,暂停被拒绝。 |
| `409` | `pause_conflict` | 并发暂停状态写入赢得了相位比较并交换;可重试。 |
| `409` | `orchestration_in_progress` | 非终态升级编排拥有作业期间暂停或恢复被拒绝。 |
| `503` | `pause_requires_durable_storage` | Hub 未使用持久化存储,无法持久化暂停相位。 |

拒绝会入审计(`job.pause` / `job.resume`,结果 `rejected`,携带上述失败码)并计入
`arkflow_job_pause_total{outcome="rejected"}`;相位转移会在 `GET /events/stream` 上发出
`job.pause_requested`、`job.paused`、`job.resume_requested`、`job.resumed` 与
`job.pause_failed`。落工件失败或超时会中止暂停、还原暂停前的恢复指针,并在作业的
`last_error` 中记录 `pause_savepoint_failed` / `pause_savepoint_timeout`。

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

### 用户与凭证 {#users-and-credentials}

Hub 在具备持久化存储时提供托管用户目录。下列每个路由都要求
**`manage_users`** 动作——仅 `admin` 持有——缺失有效凭证时返回 `401`,其他角色
返回 `403`。易失(volatile)Hub(未配置 `ARKFLOW_HUB_STORAGE`)上整组路由返回
`503 identity_store_unavailable`。

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/users` | 列出用户(纯数组)。 |
| `POST` | `/users` | 创建用户:`{user_id, display_name?, role, scopes?, oidc_subject?}` → `201` User。 |
| `GET` | `/users/{id}` | 单个用户(不存在时 `404 user_not_found`)。 |
| `PATCH` | `/users/{id}` | 局部更新:`display_name`、`role`、`scopes`、`disabled`、`oidc_subject`(空字符串清除 OIDC 绑定)。 |
| `DELETE` | `/users/{id}` | `204`;级联删除该用户的凭证与浏览器会话。 |
| `GET` | `/users/{id}/credentials` | 仅凭证元数据(纯数组)——绝不含 token 或哈希。用户不存在时返回 `404 user_not_found`。 |
| `POST` | `/users/{id}/credentials` | 签发:`{label?, expires_at_ms?}` → `201 {credential, token}`;明文 `token` **只**在此返回。 |
| `POST` | `/credentials/{id}/rotate` | `200 {credential, token}`;旧凭证在同一次存储变更中被吊销。 |
| `DELETE` | `/credentials/{id}` | 幂等吊销 → `200 {"revoked": true\|false}`。 |
| `POST` | `/users/{id}/revoke-tokens` | `200 {revoked_sessions}`;提升该用户的 JWT 吊销阈值并删除其浏览器会话。 |

用户记录:

```json
{
  "user_id": "alice",
  "display_name": "Alice",
  "role": "admin",
  "scopes": [{"resource_type": "node", "resource_id": "node-a"}],
  "disabled": false,
  "oidc_subject": "00u1abc",
  "tokens_valid_after_ms": 0,
  "created_at_ms": 1767225600000,
  "updated_at_ms": 1767225600000
}
```

`role` 为 `admin`、`operator`、`viewer` 之一(每用户一个角色);`scopes` 以与静态
凭证相同的 `{resource_type, resource_id?}` 文法收窄权限。用户 id 为 1–64 个字符,
仅 `[a-z0-9._-]`,且以小写字母或数字开头。目录最多 1024 个用户,每用户最多 16 个
活跃凭证,且一个 `oidc_subject` 只能绑定到一个用户。

凭证视图与签发响应:

```json
{"credential_id":"3f6a1c9e2b7d4a80","label":"alice-laptop","created_at_ms":1767225600000,"expires_at_ms":null,"last_used_at_ms":null,"revoked_at_ms":null}
```

```json
{"credential":{"credential_id":"3f6a1c9e2b7d4a80","label":"alice-laptop","created_at_ms":1767225600000,"expires_at_ms":null,"last_used_at_ms":null,"revoked_at_ms":null},"token":"ark_3f6a1c9e2b7d4a80_<secret>"}
```

把签发的 token 作为 `Authorization: Bearer <token>` 使用。Hub 只保存
`SHA-256(credential_id ‖ 0x00 ‖ secret)`,无法再次展示明文;本版本中
`last_used_at_ms` 为 `null`(使用时间戳是响应形状中的预留字段,尚未记录)。
吊销在该凭证的下一个请求即生效。`tokens_valid_after_ms` 是 JWT 吊销阈值:绑定
subject 的联邦令牌若 `iat` 更早(60 秒时钟偏斜容差)会被拒绝。

行为不变量:

- 凭证解析顺序为:静态 operator 凭证 → 托管 `ark_` 凭证 → 浏览器会话 → OIDC JWT。
  任一项校验失败的托管 token 一律硬性 `401`,绝不回退到 OIDC;身份存储错误会拒绝
  请求。
- 禁用、删除用户或对其调用 `revoke-tokens` 会立即使其会话失效。`revoke-tokens`
  **不会**吊销托管凭证——请逐个吊销或轮换。
- 在没有静态 operator 凭证作为应急(break-glass)时,最后一个启用的 admin 不能被
  禁用、删除或降级(`409 last_admin_required`)。
- 被接受与被拒绝的身份变更都会审计为 `user.create`、`user.update`、
  `user.delete`、`user.revoke_tokens`、`credential.create`、
  `credential.rotate` 与 `credential.revoke`,只含标量元数据。拒绝记录以同一动作
  写入 `outcome=rejected`,带真实 actor,并把稳定的问题码作为 `failure_code`
  (例如 `last_admin_required`、`credential_limit`、`user_not_found`)。

| 状态 | 错误码 | 何时出现 |
|------|--------|----------|
| `400` | `invalid_user` | 用户 id 格式错误、角色未知、名称/label 过长、scope 无法解析,或 `PATCH` 为空。 |
| `400` | `invalid_credential` | 对已吊销凭证执行轮换。 |
| `403` | `forbidden` | 已认证主体缺少 `manage_users`。 |
| `404` | `user_not_found` / `credential_not_found` | 用户或凭证不存在——包括对不存在用户列出凭证。 |
| `409` | `last_admin_required` | 在没有静态应急凭证的情况下,最后一个启用的 admin 将被禁用、删除或降级。 |
| `409` | `user_limit` / `credential_limit` | 已有 1024 个用户 / 该用户已有 16 个活跃凭证。 |
| `409` | `oidc_subject_conflict` | 该 OIDC subject 已绑定到其他用户。 |
| `503` | `identity_store_unavailable` | Hub 没有持久化存储。 |

运维生命周期见[用户与凭证](/zh-Hans/docs/operate/control-plane/users-and-credentials)。

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

当 Hub 配置了 OIDC 提供方时(参见[部署](/zh-Hans/docs/operate/control-plane/deploy#oidc-jwt-federation)),操作员 API 还会提供一个浏览器登录流:

| 方法 | 路由 | 用途 |
|--------|-------|---------|
| `GET` | `/auth/oidc/status` | 公开探测端点,报告 `login_enabled`、`authenticated` 与当前主体。 |
| `GET` | `/auth/oidc/login` | 302 跳转到身份提供方(PKCE S256 与 `nonce` 绑定到 HttpOnly cookie)。 |
| `GET` | `/auth/oidc/callback` | 用 code 换取 id_token 并创建服务端会话。 |
| `GET` | `/auth/oidc/logout` | 删除服务端会话并清除 cookie。 |

具备持久化存储时会话落盘(`cp_sessions`,schema v5):跨 Hub 重启存活,并在登出、
禁用/删除用户或调用 `POST /users/{id}/revoke-tokens` 时失效。备份与恢复有意排除
`cp_sessions`,因此恢复后的 Hub 绝不会复活登录态。易失 Hub 上会话保存在内存,
重启即登出所有人。

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
