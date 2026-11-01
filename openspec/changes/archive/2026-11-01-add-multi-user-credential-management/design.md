# Design: add-multi-user-credential-management

## Context

- **身份漏斗唯一**：`Hub::operator_principal`（`crates/arkflow-server/src/hub/operator.rs:23-58`）是中间件（`crates/arkflow-server/src/api/mod.rs:586-608`）与 handler 级 RBAC（`require_operator_action`，`api/mod.rs:1234-1277`）共同的入口；顺序是静态凭证（`parse_operator_credential`，`hub/nodes.rs:205-232`，`ct_eq` 比较）→ `session:<sid>` 浏览器会话 → OIDC JWT（`crates/arkflow-server/src/oidc.rs:116-142`）。静态凭据优先、未匹配才落到 OIDC，是既有规格明文（`hub-oidc-auth` spec「静态凭据 SHALL 优先于 OIDC」）。
- **身份状态是进程内的**：浏览器会话是 `HashMap<String,(OperatorPrincipal, Instant)>`（`oidc.rs:301`），8h 固定 TTL（`oidc.rs:334`），`OsRng` 32B session id（`oidc.rs:575-584`），登出只删本进程（`oidc.rs:596-598`）；OIDC JWT 只做签名/claims 校验，没有 jti/epoch/内省（`oidc.rs:41` 附近注释明确「availability over strict revocation」）。
- **持久化骨架**：`SCHEMA_VERSION = 3`（`storage/mod.rs:540-543`）；幂等 DDL + 探测式 `ALTER TABLE`（`sqlite.rs:2891-2975`、`postgres.rs:736`）；备份/恢复/迁移共用 `TABLE_ORDER`（`table_copier.rs:20-46`，19 项）；`cp_hub_lease` 有意排除（`tests/backup_restore.rs:324-336`）；新增表要改动的位置清单见调研报告（DDL/trait/macro/dispatch/手写委派/order/测试）。
- **RBAC 已成型**：8 个动作（`api_contract.rs:33-52`）、角色矩阵（`:79-95`）、scope 语义（`:97-118`）、principal echo（`:127-137`）。`ManageWebhooks`/`ApproveRollouts` 已示范「新增动作 + 仅 Admin」的扩展方式。
- **启动护栏**：`validate_hub_startup`（`api/mod.rs:199-252`）在 secure 模式强制 storage + operator token + node token；OIDC 不能替代（`:217-223`）。
- **Console 角色门控**：`usePermissions()` 由 `/system.principal.actions` 派生（`console/src/permissions.ts:18-24`），echo 缺失时宽松放行（`:12-15`）；构建期 token `VITE_API_TOKEN`（`api.ts:404-405`、`:443`），401 且无 token 才跳 OIDC（`:456-458`）。

## Goals / Non-Goals

**Goals:**

- 用户成为一等身份：可创建/禁用/删除，角色与 scope 落在目录中，不依赖重启改环境变量。
- 凭证可签发、可轮换、可吊销，吊销立即生效（下一个请求即 401），且秘密只存哈希、明文仅出现一次。
- OIDC 用户可绑定到目录用户；管理员可一键吊销某人的全部活跃令牌（JWT + 会话）。
- 静态凭证与既有 OIDC claims 映射保持零行为变化；存储故障 fail-closed 不放行。
- 全链审计（含真实 actor）、无秘密泄漏、Console 可用。

**Non-Goals:**

- 租户隔离、自助服务、用户组/自定义角色、密码登录、JIT 开户、jti 级单令牌吊销、纯目录启动、ReadAudit 强制与 /audit 过滤增强、CSRF token、CLI 管理子命令。

## Decisions

### D1 三张表承载身份目录

| 表 | 关键列 | 说明 |
| --- | --- | --- |
| `cp_users` | `user_id TEXT PK`、`display_name`、`role TEXT`、`scopes_json TEXT`、`disabled INTEGER`、`oidc_subject TEXT UNIQUE`、`tokens_valid_after_ms INTEGER NOT NULL DEFAULT 0`、`created_at_ms`、`updated_at_ms` | 单角色（`OperatorPrincipal.roles` 仍是一元素 vec，保持 wire 语义）；scope 存 JSON（`[{resource_type, resource_id}]`，文法复用 `parse_resource_scope`）；`user_id` 文法 `[a-z0-9][a-z0-9._-]{0,63}` 且不可变 |
| `cp_credentials` | `credential_id TEXT PK`、`user_id TEXT`（FK）、`secret_hash TEXT`、`label TEXT`、`created_at_ms`、`expires_at_ms NULL`、`last_used_at_ms NULL`、`revoked_at_ms NULL` | 只存哈希；`credential_id` 随机 16 hex，内嵌于 token 供 O(1) 定位 |
| `cp_sessions` | `session_hash TEXT PK`、`user_id TEXT NULL`、`principal_json TEXT`、`created_at_ms`、`expires_at_ms`、`revoked_at_ms NULL` | cookie 只存随机 id；行存 principal（roles/scopes 快照）+ 绑定的托管 user id |

`cp_sessions` **不入 `TABLE_ORDER`**：它是活动 bearer secret，restore 复活旧登录态是安全反模式（与 `cp_hub_lease` 排除同理，注释写明）；`cp_users`、`cp_credentials` 入 `TABLE_ORDER`（父表在前），备份/迁移/恢复自动携带，补 row-count 断言。schema `SCHEMA_VERSION` v5（排在 `add-job-pause-resume` v4 后；若顺序相反则本变更占 v4、pause 占 v5）。全部新增是 `CREATE TABLE IF NOT EXISTS`（无列级 ALTER），旧库打开即建表并盖章。

### D2 opaque 凭证：`ark_<credential_id>_<secret>`

- `secret` 为 32 字节 CSPRNG（hex/base64url，无外部依赖：`std::random`/`getrandom` 已在用）。
- 存储 `secret_hash = SHA-256(credential_id ‖ 0x00 ‖ secret)`（`sha2` 已是 workspace 依赖），比较用 `subtle::ConstantTimeEq`；高熵秘密用 SHA-256 足够（无口令字典攻击面），不引入 Argon2 重量级依赖。
- 解析：token 前缀/结构不合法 → 视为不匹配（继续走 OIDC/session，最终 401），不做存在性探测；`credential_id` 命中但哈希不匹配 → 401（不区分「不存在」与「错误」，避免枚举）。
- **明文只在签发/轮换响应体出现一次**（`201`/`200`），不落库、不写日志、不进审计；list/详情只返回 `credential_id/label/created_at/expires_at/last_used_at/revoked_at`。
- 轮换 = 同一存储命令内「新建凭证 + 旧凭证 `revoked_at_ms = now`」原子完成；响应返回新明文。
- `last_used_at_ms` **惰性更新**：仅托管凭证认证成功且距上次记录 > 5 分钟时 best-effort 写一次，避免每请求写放大（失败只 warn，不影响认证）。

### D3 认证解析顺序与 fail-closed

顺序（保持既有优先级）：静态 `operator_token` → 托管凭证（`ark_` 前缀）→ 浏览器会话（`session:` cookie 通道）→ OIDC JWT。实现上把 `operator_principal` 拆为内部 `resolve_operator_principal() -> Option<ResolvedPrincipal { principal, source }>`（用于指标）与保持签名不变的 `operator_principal()`；所有 401 语义不变（已吊销/禁用/过期/未知一律 401，不是 403，Console 的 401→登录跳转依赖它）。

- 托管凭证路径：token 结构解析 → `get_credential(credential_id)`（存储 actor，leader-only 读）→ 哈希 ct_eq → `revoked_at_ms`/`expires_at_ms` → 加载 `cp_users`（`disabled`/`tokens_valid_after` 不约束 opaque token，仅 JWT 用）→ principal = `{id: user_id, roles: vec![role], scopes}`。
- **存储故障**：配置了存储但读失败 → 本次解析视为失败（401），并记 warn + 指标 `denied`；不降级放行。volatile Hub（无存储，`Hub.storage: Option<...>`，`hub.rs:148`）→ 托管凭证路径不可用（无目录），静态/OIDC/内存会话路径照旧；管理 API 返回 503 `identity_store_unavailable`。
- 静态凭证行为逐字节不变（含畸形启动拒绝，`hub.rs:314-338`）；未配置任何托管用户/凭证时新增路径为 no-op。

### D4 浏览器会话持久化（有存储时）

- 有存储：登录回调写 `cp_sessions`（principal 快照 + `user_id` 可空 + `expires_at_ms = now + 8h`），`resolve_session` 查表并惰性拒过期；cookie 形状、`Max-Age`、`Secure` 派生完全不变（`api/diagnostics.rs:354-370`）。
- 无存储：保留现有内存 `HashMap`（本地开发/insecure_local 体验不变），但**管理 API 不可用**（见 D3），文档写明该模式没有可吊销身份。
- 语义变化（明确声明）：会话跨 Hub 重启存活（以前全员登出），且跨实例可见（同库）；`restore` 不复活会话（表不在备份里）。登出删除行；`revoke-tokens` 删除该 user 的全部行；过期清扫接入既有 maintenance sweep（默认 60s 周期）。
- 容量：`MAX_SESSIONS = 4096`；登录时先清扫过期行，仍满则拒绝新登录（503 `session_capacity`），不悄悄驱逐活跃会话。

### D5 OIDC JWT 吊销（主体级）

在校验签名/`iss`/`aud`/`exp`（既有）之后增加主体状态判定：

1. `sub` 命中 `cp_users.oidc_subject` → principal 以目录为准（role/scopes 从库取；IdP 的 role/scopes claims 不再是来源，保证目录可治理）；若 `disabled` → 401；若 `iat < tokens_valid_after_ms - 60s`（时钟偏移容差）→ 401；缺失 `iat` 且绑定了目录 → 401（无法比较吊销时间）。
2. `sub` 未绑定：保持既有 claims 映射（source=`oidc`），除非配置 `ARKFLOW_OIDC_MANAGED_USERS_ONLY=true`（默认 false，零行为变化）→ 401。
3. 单条 bearer 的 jti 级吊销不做（Non-goal）：主体级 epoch + 会话删除覆盖「吊销此人所有令牌」的真实运维诉求。

`revoke-tokens`：一次 fenced 写 `tokens_valid_after_ms = now` + 删除该 user 的会话（`cp_sessions`）。JWT 的下一个请求即 401；已存在的浏览器会话立即失效。

### D6 RBAC 与不变量

- 新动作 `ManageUsers`（Admin 仅；operator/viewer 无）；管理路由全部经 `require_operator_action(ManageUsers, "user", Some(id))`，401/403 分流与审计沿用既有漏斗。
- 不变量（全部 fail-closed，返回稳定 code）：
  - **最后一个启用 Admin 不可移除**：禁用/删除/降级某 Admin 前，若已无其他启用的 Admin，且未配置静态 `operator_token`（break-glass），拒绝 `last_admin_required`。
  - 用户 id 不可变、唯一、文法校验；`oidc_subject` 唯一（冲突 409 `oidc_subject_conflict`）。
  - 每用户凭证 ≤ 16（`credential_limit`）；用户总数 ≤ 1024（`user_limit`）。
  - 角色/scope 解析复用既有文法；scope 未知 `resource_type` 允许但文档说明仅既有资源类型生效（不做白名单，避免关死未来扩展）。
  - 轮换/吊销不存在或已吊销的凭证：幂等语义（重复吊销 200 或 404？选定：不存在 404，已吊销幂等 200）。
- 自身操作：允许 Admin 操作自己，但受最后一个 Admin 不变量约束。

### D7 管理 API 形状

| 方法/路径 | 动作 | 说明 |
| --- | --- | --- |
| `GET /api/v1/users` | ManageUsers | 列表（分页 page/page_size，默认 50/最大 200），不含秘密 |
| `POST /api/v1/users` | ManageUsers | 创建：`{user_id, display_name?, role, scopes?, oidc_subject?}` → 201 |
| `GET /api/v1/users/{id}` | ManageUsers | 详情 |
| `PATCH /api/v1/users/{id}` | ManageUsers | `display_name/role/scopes/disabled/oidc_subject` 局部更新；空 patch 400 |
| `DELETE /api/v1/users/{id}` | ManageUsers | 删除 + 级联吊销凭证与会话；保留审计行 |
| `POST /api/v1/users/{id}/credentials` | ManageUsers | 签发：`{label?, expires_at_ms?}` → 201 `{credential, token}`（唯一一次明文） |
| `GET /api/v1/users/{id}/credentials` | ManageUsers | 元数据列表 |
| `POST /api/v1/users/{id}/credentials/rotate/{credential_id}`（或 `POST /api/v1/credentials/{id}/rotate`） | ManageUsers | 原子轮换 → `{credential, token}` |
| `DELETE /api/v1/credentials/{id}` | ManageUsers | 吊销（幂等） |
| `POST /api/v1/users/{id}/revoke-tokens` | ManageUsers | bump epoch + 删会话 → `{revoked_sessions}` |

- 错误码：`identity_store_unavailable`(503)、`invalid_user`/`invalid_credential`(400)、`user_not_found`/`credential_not_found`(404)、`last_admin_required`/`credential_limit`/`user_limit`/`oidc_subject_conflict`(409)、`forbidden`(403)。
- 所有响应/日志/审计都不含 `token` 与 `secret_hash`；审计 `message` 只放标量（user_id、credential_id、role 变更摘要）。

### D8 持久化与 HA

- 所有身份写命令走 **fenced** 存储命令（租约 epoch 栅栏，`hub-ha` spec；`storage/mod.rs:556-587` 宏注释），standby 不能写；读只在 leader 路由上发生（standby 对 operator 路由 503，`hub-ha` spec:115-117）。
- **不做读缓存**（v1）：吊销立即生效优先于吞吐；每次 operator 请求新增最多一到两次 actor 往返（SQLite 微秒级、PG 毫秒级，相对 handler 的存储操作可接受）。若未来加缓存，必须显式声明最大失效延迟——作为 Non-goal 留给后续。
- 迁移/备份/恢复：`TABLE_ORDER` 增两表；`backup` 快照会自动含身份目录（凭证只含哈希，文档按数据库同等密级保管）；`restore` 后会话全部失效（用户需重新登录），这是安全上更优的默认。
- 兼容：旧二进制读 v5 库被既有 `SchemaTooNew` 拒绝；新二进制读 v3/v4 库自动建表。回滚需恢复备份（与既往 schema 升级一致）。

### D9 Console

- 新增「用户与凭证」面板（Settings 内，Hub 模式 + `can('manage_users')` 才渲染）：用户列表/创建/编辑角色与 scope/禁用/删除；凭证列表/签发（一次性弹层展示+复制）/轮换/吊销；「吊销全部令牌」按钮走确认组件。
- **运行期凭证输入**：首次 401（非 OIDC 模式）或手动切换身份时输入 Bearer token，存 `sessionStorage`（关闭标签页即失效；不落 localStorage），支持清除；`VITE_API_TOKEN` 降级为可选默认值。401 处理保持「有 OIDC 登录流则跳转，否则提示输入凭证」。
- 门控：`usePermissions().can('manage_users')`；`OperatorAction` TS 联合类型同步新增 `manage_users`；`settings.credentialsNote` 文案改写为「凭证由管理员签发，界面不持久化」。
- i18n：en/zh 全量；`zh.ts` 的 `satisfies Record<keyof typeof en, string>` 保证编译期对齐。

### D10 审计 actor 保真

`record_job_operation_audit` 的 actor 硬编码（`hub/observability.rs:296`）必须改为调用方传入 principal id；本变更新增的审计全部显式传 actor。与 `add-job-pause-resume` 的同一改造协调：先落地者完成函数签名改造，后落地者只接线 + 补断言，避免双实现。

## Risks / Trade-offs

- **锁死风险（最大运维风险）**：误删/降级最后一个 Admin。缓解：`last_admin_required` 不变量；静态 env token 作为 break-glass（即使目录被清空仍可进入）；文档写明「保留 break-glass」。测试覆盖「只剩一个 Admin 时禁用/删除/降级均 409」。
- **首管理员 bootstrap**：安全模式仍依赖 env token（Non-goal 纯目录启动）。文档把 env token 定位为 break-glass，而不是日常凭证；Console 文案同步。
- **每请求存储读**：PG 上毫秒级；高 QPS 场景有成本。取舍：v1 不缓存，先保证吊销即时；若后续加缓存，schema 不变、仅换读路径。
- **会话持久化是行为变化**：Hub 重启后浏览器不再强制重登。安全上可接受（会话本应跨重启），但必须写进 CHANGELOG/文档；restore 不复活会话这一边界也要写。
- **秘密一次性交付**：明文只出现一次，丢失只能轮换。UI 必须提示；API 文档写明。
- **401 与存储故障同码**：存储抖动期间所有托管凭证请求 401（fail-closed）。文档与指标（`denied` 计数）让运维可辨别；不引入 503 是因为会破坏 Console 401→登录流与既有 401 语义。
- **CSRF**：管理端点接受 session cookie（SameSite=Lax，跨站 POST 不带 cookie）；不加 CSRF token 的残余风险记入 Non-goal。若后续引入跨站嵌入需求再评估。
- **限流桶随轮换更替**：每次轮换产生新 bucket key，桶表有界 1024 + 惰性回收；极端高频轮换可挤占，但轮换本身受 ManageUsers 权限与限流器约束，风险低（文档记录）。

## Migration Plan

1. `SCHEMA_VERSION` 目标 v5（若 `add-job-pause-resume` 尚未落地则占 v4 并在 tasks 里显式记录实际值）；只新增 `CREATE TABLE IF NOT EXISTS`，无列级 ALTER。
2. 新表在 SQLite DDL（`storage/sqlite.rs:2573-2890`）与 PG DDL（`storage/postgres.rs:430-758`）同时声明 + 索引（`cp_credentials(user_id)`、`cp_sessions(user_id)`、`cp_sessions(expires_at_ms)`、`cp_users(oidc_subject)` 唯一）。
3. `storage_ops!` 条目 + `StorageBackend` trait 方法 + 两后端实现 + `ControlPlaneStore` 手写委派；写命令带 fenced epoch。
4. `TABLE_ORDER` 增 `cp_users`、`cp_credentials`；`cp_sessions` 排除并注释；`IDENTITY_COLUMNS` 不变（TEXT 主键）。
5. 测试：表存在性、双后端 contract（`ARKFLOW_TEST_POSTGRES_URL` 门控）、backup/restore/migrate 行数等价、schema 版本盖章与拒绝更新库。
6. 部署：升级顺序无状态依赖（先升二进制再改配置）；启用目录后建议：创建至少两名 Admin → 给每人签发个人凭证 → 把 env token 缩回 break-glass 用途（文档步骤）。
