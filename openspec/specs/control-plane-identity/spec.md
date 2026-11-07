## Purpose

Define operator identity, resource-scoped authorization, and actor-aware control-plane auditing.
## Requirements
### Requirement: Authenticated operator principal

The control plane SHALL resolve every operator request to an authenticated principal, except explicitly configured health and liveness routes, and SHALL keep Agent authentication separate from operator authentication.

#### Scenario: Reject an unauthenticated mutation
- **WHEN** an unauthenticated request attempts to change a Stream, configuration, node mode, or rollout
- **THEN** the service returns 401 and does not create or mutate a control-plane resource

### Requirement: Resource-scoped RBAC

The service SHALL authorize actions by principal, role, action, and resource scope, including fleet, node, Stream, configuration, rollout, webhook subscription, and audit read scopes. The action set SHALL include `ManageWebhooks` (webhook subscription replacement and test delivery), held by the admin and operator roles and denied to the viewer role, at fleet scope.

#### Scenario: Deny an out-of-scope action

- **WHEN** an authenticated principal lacks permission for the target node or requested action
- **THEN** the service returns 403, leaves desired state and operations unchanged, and audits the denial

#### Scenario: Viewer cannot manage webhooks

- **WHEN** a viewer-role principal calls a webhook subscription replacement or test-delivery route
- **THEN** the service returns 403, the subscription set is unchanged, and the denial is audited

#### Scenario: Operator manages webhooks

- **WHEN** an operator-role principal calls a webhook subscription replacement route
- **THEN** the action is authorized and processed under the `ManageWebhooks` action

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

结构化 operator 凭据（`id|role|secret[|scopes]`）解析失败时（段数不足、未知角色、空 id 或空 secret）MUST 拒绝该凭据：启动期校验 SHALL 发现含分隔符但畸形的凭据并拒绝启动（错误信息脱敏，不回显完整密钥），请求期该凭据 MUST 视为不可匹配。不含分隔符的纯静态 token 模式 SHALL 保持既有行为（整串作为 Admin 密钥）。系统 MUST NOT 在解析失败时把整串配置静默当作 Admin 凭据。托管凭证存储（`cp_credentials`）SHALL 是静态配置之外唯一的第二条 operator 凭据来源：其行 MUST 只由 Hub 的签发路径写入（服务端生成的 id、秘密哈希与非空用户绑定），配置注入、外部直写或完整性校验失败的行 MUST 视为不可匹配、不得授予任何 principal；启动期校验仍只作用于静态配置字段，托管存储的完整性由存储 schema 约束与写入路径校验保证。

#### Scenario: 畸形结构化凭据拒绝启动

- **WHEN** operator 凭据配置含 `|` 但角色名未知（如 `alice|typo|secret`）并启动 Hub
- **THEN** 启动失败并列出该畸形凭据（脱敏），该字符串不再构成任何有效凭据

#### Scenario: 纯静态 token 模式保持

- **WHEN** operator token 配置为不含 `|` 的普通字符串
- **THEN** 该字符串按既有语义作为 Admin 静态凭据参与鉴权

#### Scenario: 启动期校验覆盖静态凭据配置的唯一位置

- **WHEN** Hub 的 operator 凭据（单条静态字段 `operator_token`）为「含分隔符但解析失败」
- **THEN** 启动校验拒绝启动并列出该畸形凭据（脱敏），静态配置文件不存在其他 operator 凭据配置位置，且托管凭证存储只能经 Hub 签发路径写入

#### Scenario: 托管存储中的异常行不构成凭据

- **WHEN** 托管凭证表存在一行缺失秘密哈希、缺失用户绑定或用户已不存在的记录
- **THEN** 该行在请求期不可匹配，携带对应 token 的请求按未认证处理（401）

### Requirement: Job pause and resume mutations are audited

The Hub SHALL record an audit event for every accepted or rejected Job pause and resume mutation, with the actor, action (`job.pause` / `job.resume`), target Job resource, target nodes where applicable, correlation id, outcome, and a stable failure code where applicable. Phase-transition failures that abort a pause SHALL be recorded with a stable failure code on the same lifecycle action. Audit messages MUST NOT contain credentials, Job configuration bodies, or secret values; they SHALL be limited to scalar operation metadata.

#### Scenario: A Job pause is accepted

- **WHEN** the Hub accepts a Job pause mutation
- **THEN** an audit record with action `job.pause`, resource type `job`, target Job id, actor, correlation id, and outcome `accepted` is persisted

#### Scenario: A Job pause is rejected

- **WHEN** a Job pause mutation is rejected (for example because the Job is not running or an orchestration owns it)
- **THEN** an audit record with action `job.pause`, outcome `rejected`, and a stable failure code is persisted without embedding the Job spec

#### Scenario: A failed pause is recorded

- **WHEN** a pause aborts because its savepoint failed or timed out
- **THEN** an audit record carrying the stable abort failure code is persisted for the Job

#### Scenario: A Job resume is audited

- **WHEN** an authorized operator resumes a paused Job or the resume is rejected because the Job is not paused
- **THEN** an audit record with action `job.resume` is persisted with the matching outcome

### Requirement: Managed operator principals SHALL be directory-backed

An operator request presented with a managed credential SHALL be resolved to a principal whose id is the owning user id, whose roles are the user's single configured role, and whose resource scopes are the user's configured scopes. The action set SHALL include `ManageUsers`, held by the admin role only and denied to the operator and viewer roles. Disabled users SHALL NOT authenticate. The principal echo SHALL continue to report the resolved id, roles, and the caller's authorized actions so console gating stays complete.

#### Scenario: Managed credential maps to its user principal

- **WHEN** a user with the operator role and a node-scoped resource scope authenticates with a managed credential
- **THEN** the resulting principal has that user's id, the operator role, and that scope, and the response is authorized accordingly

#### Scenario: Disabled user cannot authenticate

- **WHEN** a disabled user presents a still-unrevoked managed credential
- **THEN** the Hub returns 401 and authorizes no action

#### Scenario: ManageUsers is admin-only

- **WHEN** a principal's role is operator or viewer
- **THEN** the `ManageUsers` action is absent from the action set and user administration routes return 403

#### Scenario: Principal echo reflects directory roles

- **WHEN** a managed principal calls the system endpoint
- **THEN** the echoed principal carries the user id, the directory role, and the actions allowed for that role

### Requirement: User and credential mutations SHALL be audited

Every accepted or rejected user-directory, credential, and token-revocation mutation SHALL be recorded with the acting principal, action, target resource, correlation id, outcome, and a stable failure code where applicable. Audit records MUST NOT contain credential plaintext, secret hashes, authorization headers, or secret configuration values; they SHALL be limited to stable identifiers and scalar metadata.

#### Scenario: Rejected user administration is audited

- **WHEN** a principal without `ManageUsers` calls a user creation route
- **THEN** an audit record identifies the requesting principal, the attempted action, the target resource, the correlation id, and outcome `rejected`

#### Scenario: Identity audit carries no secret material

- **WHEN** a credential is issued, rotated, or revoked
- **THEN** no audit record contains the token plaintext or its stored hash

