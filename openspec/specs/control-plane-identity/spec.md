## Purpose

Define operator identity, resource-scoped authorization, and actor-aware control-plane auditing.
## Requirements
### Requirement: Authenticated operator principal

The control plane SHALL resolve every operator request to an authenticated principal, except explicitly configured health and liveness routes, and SHALL keep Agent authentication separate from operator authentication.

#### Scenario: Reject an unauthenticated mutation
- **WHEN** an unauthenticated request attempts to change a Stream, configuration, node mode, or rollout
- **THEN** the service returns 401 and does not create or mutate a control-plane resource

### Requirement: Resource-scoped RBAC

The service SHALL authorize actions by principal, role, action, and resource scope, including fleet, node, Stream, configuration, rollout, and audit read scopes.

#### Scenario: Deny an out-of-scope action
- **WHEN** an authenticated principal lacks permission for the target node or requested action
- **THEN** the service returns 403 and leaves desired state, operations, and audit state unchanged

### Requirement: Actor-aware audit

Every accepted or rejected control-plane mutation SHALL record the principal, action, target resource, correlation ID, outcome, and stable failure code when applicable. Audit records MUST NOT contain credentials, authorization headers, or secret configuration values.

#### Scenario: Audit a denied request
- **WHEN** a principal submits a validly authenticated but unauthorized mutation
- **THEN** an audit record identifies the principal, target, denied action, correlation ID, and reason without exposing secrets

### Requirement: Job lifecycle mutations are audited

The Hub SHALL record an audit event for every accepted or rejected distributed-Job lifecycle mutation — the `job_start`, `job_stop`, `job_checkpoint`, and `job_savepoint` operations — with the actor, action, target Job resource, node targets, correlation ID, outcome, and stable failure code where applicable. Recovery IS part of the start mutation and SHALL appear in its audit record rather than as a separate event; command-result operations (`job_checkpoint_commit`, `job_savepoint_commit`) are Agent reports, not operator mutations, and are not covered by this requirement. Audit messages MUST NOT contain credentials or Job configuration bodies; they SHALL be limited to scalar operation metadata.

#### Scenario: A Job start is accepted

- **WHEN** the Hub accepts a Job start mutation for placement
- **THEN** an audit record with action `job.start`, resource type `job`, the target Job ID, actor, correlation ID, and outcome `accepted` is persisted

#### Scenario: A Job stop is rejected

- **WHEN** a Job stop mutation is rejected (for example by fencing or capacity)
- **THEN** an audit record with action `job.stop` and a stable failure code is persisted, and the record exposes the rejection reason without embedding the Job configuration

#### Scenario: Audit queries resolve Job history

- **WHEN** an authorized operator lists audit events filtered by the Job resource ID
- **THEN** the response includes the Job lifecycle audit records within the retention bound

### Requirement: Operator 认证 SHALL 在路由层统一执行

Hub 的 operator 路由 SHALL 在路由层通过中间件统一执行 token 认证——新增 operator 路由自动受保护，无需逐 handler 手写检查。白名单路径（健康探针、metrics、组件目录、OIDC 端点、agent 路由）SHALL 豁免中间件检查（agent 路由的 node token 认证在 hub 方法内部完成）。Handler 级的 RBAC scope 检查（`require_operator_action`）SHALL 保留在 handler 内（中间件只保证"是有效 operator"，scope 是 handler 语义）。

#### Scenario: 无 token 的 operator 路由全部 401

- **WHEN** 对任意 operator 路由发不带 Authorization 头的请求（矩阵测试遍历全部路由 × 方法）
- **THEN** 全部返回 401（与手写检查时的响应逐位一致）

#### Scenario: 新增路由自动受保护

- **WHEN** 向 hub_router 添加一个新的 operator 路由（无需手写 auth 检查）
- **THEN** 中间件自动对其执行 token 认证——忘加检查不再产生认证绕过

#### Scenario: 白名单路由不受影响

- **WHEN** 访问健康探针、metrics、组件目录或 OIDC 端点（不带 token）
- **THEN** 不返回 401（这些路由的设计就是公开的）

### Requirement: Operator credential parsing SHALL fail closed
结构化 operator 凭据（`id|role|secret[|scopes]`）解析失败时（段数不足、未知角色、空 id 或空 secret）MUST 拒绝该凭据：启动期校验 SHALL 发现含分隔符但畸形的凭据并拒绝启动（错误信息脱敏，不回显完整密钥），请求期该凭据 MUST 视为不可匹配。不含分隔符的纯静态 token 模式 SHALL 保持既有行为（整串作为 Admin 密钥）。系统 MUST NOT 在解析失败时把整串配置静默当作 Admin 凭据。

#### Scenario: 畸形结构化凭据拒绝启动
- **WHEN** operator 凭据配置含 `|` 但角色名未知（如 `alice|typo|secret`）并启动 Hub
- **THEN** 启动失败并列出该畸形凭据（脱敏），该字符串不再构成任何有效凭据

#### Scenario: 纯静态 token 模式保持
- **WHEN** operator token 配置为不含 `|` 的普通字符串
- **THEN** 该字符串按既有语义作为 Admin 静态凭据参与鉴权

#### Scenario: 启动期校验覆盖凭据配置的唯一位置
- **WHEN** Hub 的 operator 凭据（单条静态字段 `operator_token`）为「含分隔符但解析失败」
- **THEN** 启动校验拒绝启动并列出该畸形凭据（脱敏），且不存在可绕过该校验的其他 operator 凭据配置位置

