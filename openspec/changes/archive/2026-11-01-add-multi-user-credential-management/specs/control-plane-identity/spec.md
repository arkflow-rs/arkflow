# Delta Spec: control-plane-identity

## MODIFIED Requirements

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

## ADDED Requirements

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
