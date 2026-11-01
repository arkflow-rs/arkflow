# Proposal: add-multi-user-credential-management（控制面多用户与凭证管理面）

## Why

控制面的身份模型目前是「一条静态凭证 + 可选 OIDC 联邦」，**没有用户目录、没有凭证生命周期、没有吊销**：

- **只能配置一条 operator 凭证**：`HubConfig.operator_token`（`crates/arkflow-server/src/hub.rs:70`）来自单一环境变量 `ARKFLOW_OPERATOR_TOKEN`（`crates/arkflow-server/src/bin/arkflow-server.rs:142`）；结构化文法 `id|role|secret[|scopes]` 只解析一条（`crates/arkflow-server/src/hub/nodes.rs:205-232`），纯静态串则整体作为 Admin 密钥（同文件 `:208-211`）。多个人/多个角色共用一条凭证，无法区分操作者，也无法单独吊销某个人。
- **凭证无法签发/轮换/吊销**：`operator_principal` 每次请求只做「配置串常量时间比较 → OIDC → 会话」（`crates/arkflow-server/src/hub/operator.rs:24-62`）；改动凭证必须改环境变量并重启 Hub。审计里的 `actor` 因此只能来自配置串自带的 id 或 IdP subject。
- **JWT / 会话没有吊销机制**：OIDC bearer 校验后把 claims 映射成 `OperatorPrincipal`（`openspec/specs/hub-oidc-auth/`），代码显式声明「known kids stay usable forever (availability over strict revocation)」（`crates/arkflow-server/src/oidc.rs:41` 附近注释）；浏览器会话是进程内 `HashMap`（`crates/arkflow-server/src/oidc.rs:301`、`:580`），8h 固定 TTL、Hub 重启即丢、无法按用户批量注销。PLANNING 第十一节 11.2-5 把「用户 CRUD + token 签发/吊销/轮换；前置 JWT 吊销机制」列为缺口（11.4 P3 行）——没有吊销，凭证生命周期管理是空谈。
- **RBAC 已就绪但无目录承载**：角色（admin/operator/viewer）、动作（`OperatorAction`，`crates/arkflow-server/src/api_contract.rs:33-52`，全量常量 `ALL` 见 `:54-68`；含 `ManageWebhooks`/`ApproveRollouts` 的职责分离先例）、角色→动作矩阵（`:79-95`）与资源 scope 文法（`scope = type=id`，`can_scope` 见 `:97-118`，解析器 `hub/nodes.rs:234-243`）都已实现，但只能表达在一条配置串上；`/system` principal echo（`api_contract.rs:127-137`）已把 `{id, roles, actions}` 返回给 Console（`console/src/permissions.ts:18-24` 的 `usePermissions()`），前端具备做角色感知门控的基础。
- **审计归因已存在硬编码缺陷**：Job 生命周期审计把 `actor` 写死为 `"operator"`（`crates/arkflow-server/src/hub/observability.rs:296`），多用户后无法归因；凭证与用户操作必须携带真实 principal。
- **会话与 CSRF 形状**：浏览器会话 cookie 为 `HttpOnly; SameSite=Lax; Path=/`（`crates/arkflow-server/src/api/diagnostics.rs:354-370`），无独立 CSRF token；管理端点沿用同一认证面。
- **控制面已有可复用的持久化与治理骨架**：storage actor + 21 张 `cp_*` 表（19 张在 `TABLE_ORDER`，另有 `cp_hub_lease`、`cp_schema_meta`；`SCHEMA_VERSION = 3` 见 `crates/arkflow-server/src/storage/mod.rs:540-543`）、打开时幂等 DDL + 探测式 `ALTER TABLE` 迁移（`storage/sqlite.rs:2891-2995`、`postgres.rs:736`）、备份/恢复与迁移共用 `TABLE_ORDER`（`crates/arkflow-server/src/storage/table_copier.rs:20-46`）、审计表与动作漏斗（`crates/arkflow-server/src/hub/observability.rs:281`）、限速按凭证哈希分桶（`crates/arkflow-server/src/api/rate_limit.rs:58-67`，近期 #1344）。
- **Console 只有构建期凭据**：API token 来自 `VITE_API_TOKEN`（`console/src/api.ts:404-405`、注入 `:443`），运行期无法切换身份、也无法在界面上签发/分发个人凭证——多用户落地面必须在 Console 提供运行期凭证输入，否则「每人一条凭证」在 UI 上不可用。

本变更交付**目录化的用户与凭证管理面**：用户 CRUD、按用户签发/轮换/吊销 opaque 凭证、用户级 token 吊销（含 OIDC JWT 与浏览器会话）、RBAC 扩展 `ManageUsers`、审计与 Console 管理页；静态凭证与 OIDC 现有语义作为兼容层完整保留。

## What Changes

- **用户目录（新增 `cp_users`）**：`user_id`（稳定 id）、`display_name`、`role`（admin/operator/viewer）、`scopes`（沿用 `type=id` 文法）、`disabled`、`oidc_subject`（可选，SSO 绑定）、`tokens_valid_after_ms`（按用户吊销 JWT 的阈值）、`created_at_ms`/`updated_at_ms`。bounds：用户数上限、id 文法校验、角色/scope 复用既有文法。
- **凭证（新增 `cp_credentials`）**：`credential_id`、`user_id`、`secret_hash`（SHA-256，含 credential id 前缀，常量时间比较；明文只在签发响出现一次）、`label`、`created_at_ms`、`expires_at_ms`（可选）、`last_used_at_ms`、`revoked_at_ms`。签发/轮换/吊销 API；轮换 = 新凭证原子生效 + 旧凭证立即吊销；每用户凭证数有界。
- **会话持久化与吊销（新增 `cp_sessions`）**：OIDC 浏览器登录写入带过期时间的服务端会话行，`arkflow_session` cookie 只存随机 id；登出删除行；用户禁用/删除/`revoke-tokens` 删除其全部会话。语义与今日 8h TTL 一致，但跨 Hub 重启存活、可被立即吊销。
- **JWT 吊销**：OIDC bearer 在校验签名/`iss`/`aud`/`exp` 之后，SHALL 增加「主体状态」判定——若 `sub` 绑定到托管用户：用户被禁用、或令牌 `iat` 早于 `tokens_valid_after_ms`（含时钟偏移容差）→ 401；未绑定用户时保持既有 claims 映射（可配置 `managed_users_only` 收紧为目录唯—来源，默认关闭以保持零行为变化）。新增 `POST /api/v1/users/{id}/revoke-tokens`（bump `tokens_valid_after_ms` + 删除会话）作为面向用户的「吊销此人所有活跃令牌」入口；单条 bearer 的 jti 级吊销列为 Non-goal。
- **认证解析顺序（fail-closed）**：静态 `operator_token`（既有，break-glass，优先级不变）→ 托管凭证（opaque token 查表 + 常量时间哈希比较）→ OIDC bearer（含上述主体吊销判定）→ 浏览器会话（查 `cp_sessions`）。存储读失败时该来源视为不匹配（整体 401），不降级放行；无托管凭证/无会话时行为与现状逐字节一致。
- **RBAC 扩展**：新增 `OperatorAction::ManageUsers`（仅 Admin）；用户/凭证/会话管理路由全部要求该动作；动作集同步 `OperatorAction::ALL`（`crates/arkflow-server/src/api_contract.rs:54-68`）与 `/system` principal echo，Console 的 `usePermissions` 门控依赖此口径；TS 侧 `console/src/api.ts:84-93` 的联合类型同步。
- **启动护栏**：secure 模式（非回环）仍要求静态 operator 凭证作为 bootstrap/break-glass（`crates/arkflow-server/src/api/mod.rs:199-252`，OIDC 不能替代 `operator_token` 见 `:217-223`；`secure-durable-control-plane` 语义不变）；托管用户要求 durable storage——无 durable storage 的 volatile Hub 尝试用户/凭证管理 SHALL 以稳定错误拒绝（内存态不允许持久身份），OIDC 会话在该模式下保留内存兜底以维持现有本地开发体验。
- **审计与可观测**：`user.create`/`user.update`/`user.disable`/`user.enable`/`user.delete`/`credential.create`/`credential.rotate`/`credential.revoke`/`user.revoke_tokens` 全量审计（接受与拒绝，仅标量元数据，绝不含明文/哈希/令牌）；认证结果 counter `arkflow_operator_auth_total{source,outcome}`（source ∈ static|managed|oidc|session，outcome ∈ ok|denied，低基数）；会话/凭证过期清扫接入既有 maintenance sweep；同时修复 Job 审计 actor 硬编码（`crates/arkflow-server/src/hub/observability.rs:296`），与 `add-job-pause-resume` 的同一改造协调（谁先落地谁完成，另一变更只做接线与断言）。
- **Console**：Settings 增「用户与凭证」管理面（仅 `ManageUsers` 可见/可用）：用户列表与创建、角色/scope 编辑、禁用/启用/删除、凭证签发（一次性展示明文并可复制）、轮换、吊销、revoke-tokens；破坏性操作走既有确认组件；新增**运行期凭据输入**（首次 401 或手动「切换凭证」时输入 Bearer token，存于 sessionStorage、可清除，不再依赖构建期 `VITE_API_TOKEN`），并改写 Settings 里「凭证只来自构建期环境」的说明文案；en/zh 字典。
- **迁移协调**：新增表纳入 `TABLE_ORDER`（`cp_users`、`cp_credentials`）并补 row-count/roundtrip 断言；`cp_sessions` **有意排除**在备份之外（会话是活动 bearer secret，restore 不应复活登录态，安全理由写入注释与文档）；schema `SCHEMA_VERSION` 递增（目标 v5，排在 `add-job-pause-resume` 的 v4 之后；若落地顺序相反则占下一个空闲版本），SQLite/PostgreSQL 双后端 DDL + 打开时迁移，旧库自动补齐。
- **文档**：认证与身份（en/zh）、API 参考（用户/凭证路由与权限表）、部署页（bootstrap 与断接凭证轮换流程）、`SECURITY.md` 凭证保管与吊销语义、`CHANGELOG.md`。

## Non-goals

- **租户/团队隔离**（多租户数据面隔离、按租户配额）——PLANNING 将其与本项同批评估，但需要资源模型全局改造，另立专项。
- **单条 bearer 的 jti 级吊销**——面向用户的是「吊销该用户所有令牌」；单条 OIDC token 的精确吊销需要 IdP 侧配合或全量 jti 目录，收益/成本不匹配。jti 表结构不建。
- **纯目录启动（去掉静态 operator 凭证）**——secure 模式仍要求静态 break-glass 凭证（`crates/arkflow-server/src/api/mod.rs:217-223`）；bootstrap 顺序（先开库、再从库读管理员）会改变启动护栏语义，另行评估。
- **自助服务**（用户自己管理自己的凭证、修改自己的资料/角色）——v1 仅 Admin 管理；自助改为后续增量。
- **用户组/团队、权限继承、审批工作流变更**——角色仍是三角色枚举 + scope，不引入自定义角色/权限集。
- **密码认证/本地登录**——凭证仍是高熵 opaque token（无口令、无登录页）。
- **OIDC 用户自动开户（JIT provisioning）**——`oidc_subject` 绑定需管理员显式创建/更新，未绑定主体维持 claims 映射。
- **Console 之外的管理 CLI 子命令**——管理动作全部经 HTTP API；CLI 化按需后补。
- **改名/合并用户、审计历史迁移**——用户 id 不可变；删除用户保留既有审计行的 actor 字符串。
- **审计读取的 RBAC 强制（`ReadAudit` 目前从未被强制，`crates/arkflow-server/src/api/events.rs:142-159`）与 `/audit` 的 actor/action/时间过滤增强**——已知缺口（会改变 operator 角色的现有可达性），另立专项；本变更只保证自己的写审计不泄漏凭据。
- **独立 CSRF token / step-up 认证**——管理端点沿用现有 `HttpOnly; SameSite=Lax` 会话语义；CSRF token 与二次认证另行评估。

## Capabilities

### New Capabilities

- `control-plane-credentials`: 控制面用户目录与凭证生命周期契约——用户 CRUD 与不变量、opaque 凭证签发/轮换/吊销、会话持久化与吊销、JWT 主体吊销判定、认证解析优先级与 fail-closed、边界与保留、审计/指标/备份出口。

### Modified Capabilities

- `control-plane-identity`（ADDED）：托管 principal 的来源与 `ManageUsers` 动作、用户/凭证变更审计。
- `hub-oidc-auth`（ADDED）：OIDC 主体吊销判定、托管用户绑定、会话持久化后的登录/登出语义、`managed_users_only` 开关。
- `secure-durable-control-plane`（ADDED）：托管身份要求 durable storage；存储不可用时身份操作 fail-closed。
- `control-plane-console`（ADDED）：用户与凭证管理面及角色感知门控。

## Impact

- `crates/arkflow-server/src/storage/`：`mod.rs`（三张新表 DTO + 方法 + `SCHEMA_VERSION` v5）、`sqlite.rs`/`postgres.rs`/`postgres_methods.rs`（DDL + 迁移 + 方法）、`table_copier.rs`（`TABLE_ORDER` 增 `cp_users`、`cp_credentials`；`cp_sessions` 显式排除并注释理由）。
- `crates/arkflow-server/src/hub/`：`operator.rs`（解析顺序 + 托管凭证 + 主体吊销判定）、新 `users.rs`（用户/凭证/会话 CRUD + 不变量 + 审计）、`hub.rs`（配置/装配）、`observability.rs`（审计动作）、`lifecycle.rs`/maintenance（过期清扫）。
- `crates/arkflow-server/src/oidc.rs`：会话改持久化、登录/回调/登出适配、JWT 主体状态检查（禁用/epoch/`managed_users_only`）。
- `crates/arkflow-server/src/api/`：新 `users.rs`（管理路由 + authz）、`mod.rs`（装配与 `OPERATOR_AUTH_EXEMPT` 复核）、`api_contract.rs`（`ManageUsers` + DTO）、`rate_limit.rs`（键空间复核：托管凭证按凭证哈希，无需新逻辑但要测）。
- `console/src/`：`features/settings.tsx` 或新 `features/users.tsx`、`api.ts`、`features/types.ts`、`i18n/{en,zh}.ts`。
- `docs/`：`docs/docs/operate/control-plane/overview.md`、`deploy.md`、`docs/docs/reference/api.md`、`docs/docs/reference/configuration.md`（env/配置）、必要时新增 `docs/docs/operate/control-plane/authentication.md` + zh-Hans 对应树；`SECURITY.md`、`CHANGELOG.md`。
- 依赖：零新增（`sha2`、`subtle`、随机数生成已在用）。
- 兼容性：静态凭证与 OIDC claims 映射语义逐字节保留；无托管用户时所有既有认证路径不变；新增表与路由均 additive；`schema` 升级沿用「新二进制可读旧库、旧二进制拒绝新库」既有规则。
