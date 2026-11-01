# Tasks: add-multi-user-credential-management

## 1. 存储与 schema

- [x] 1.1 `crates/arkflow-server/src/storage/mod.rs`：新增 `UserRecord`、`CredentialRecord`、`SessionRecord` DTO + 方法签名（`list_users/get_user/upsert_user/update_user/delete_user`、`create_credential/get_credential/list_credentials/revoke_credential/rotate_credential`、`insert_session/get_session/delete_session/delete_user_sessions/prune_expired_sessions`）；`SCHEMA_VERSION` 递增到下一个空闲版本（若 `add-job-pause-resume` 已落地则 v5）；登记 `storage_ops!` 条目（写命令走 fenced 变体）与 `StorageBackend` trait
- [x] 1.2 `storage/sqlite.rs`：三表 DDL + 索引（`cp_credentials(user_id)`、`cp_sessions(user_id)`、`cp_sessions(expires_at_ms)`、`cp_users(oidc_subject)` UNIQUE），全部 `CREATE TABLE IF NOT EXISTS`；方法实现；`ControlPlaneStore` 手写委派；单测：新库建表、旧库打开自动建表并盖章、写入/读取/唯一约束/删除级联
- [x] 1.3 `storage/postgres.rs` + `postgres_methods.rs`：同构 DDL（含约束与索引）+ 方法 + 门控 PG contract 测试（`ARKFLOW_TEST_POSTGRES_URL`）；断言 fenced 写命令在 stale epoch 下被拒
- [x] 1.4 `storage/table_copier.rs`：`TABLE_ORDER` 追加 `cp_users`、`cp_credentials`（父表先行），`cp_sessions` 显式排除并注释安全理由；`IDENTITY_COLUMNS` 无需变更（TEXT 主键）；`tests/backup_restore.rs` 补身份表行数/值等价断言（sqlite→sqlite、sqlite→pg 门控、pg→sqlite 门控），restore 后会话表为空

## 2. 用户与凭证领域逻辑

- [x] 2.1 `crates/arkflow-server/src/hub/users.rs`（新模块）：用户 CRUD；id 文法校验、role 解析、scope 解析复用 `parse_resource_scope`（`hub/nodes.rs:234-243`）、`oidc_subject` 唯一冲突、分页列表
- [x] 2.2 凭证：`ark_<credential_id>_<secret>` 生成（CSPRNG 32B）、`SHA-256(credential_id ‖ 0x00 ‖ secret)` 存储、常量时间比较、一次性明文返回、签发/轮换（原子）/吊销/过期校验、`last_used_at_ms` 惰性更新（>5min 才写，best-effort）
- [x] 2.3 不变量：最后一个启用 Admin 保护（禁用/删除/降级前检查其他启用 Admin 或静态 `operator_token`，否则 `last_admin_required`）；用户/凭证数量上限；删除用户级联吊销凭证+会话；单测覆盖每个不变量正反例
- [ ] 2.4 审计漏斗：`record_identity_audit(actor, action, resource, outcome, failure_code)`（复用 `record_audit_event`，`hub/observability.rs:281` 范本）；与 `add-job-pause-resume` 协调修复 `record_job_operation_audit` 的 actor 硬编码（`observability.rs:296`）——先落地者改签名，本任务只接线并断言

## 3. 认证解析（托管凭证）

- [x] 3.1 `crates/arkflow-server/src/hub/operator.rs`：内部 `resolve_operator_principal() -> Option<ResolvedPrincipal{principal, source}>`，顺序 static → managed → oidc → session；`operator_principal()` 保持签名不变（既有调用点零改动）；source 供指标使用
- [x] 3.2 托管凭证分支：token 结构解析 → 按 credential_id 读凭证 → ct_eq → 过期/吊销/用户禁用判定 → principal（role/scopes 来自目录）；token 结构不合法视作不匹配；不存在/哈希错/吊销/过期一律 401
- [x] 3.3 fail-closed：有存储但读失败 → 该来源失败（401）+ warn + 指标 `denied`；volatile Hub（无存储）→ 托管路径不可用、静态/OIDC/内存会话行为不变；管理 API 返回 `identity_store_unavailable`（503）
- [x] 3.4 测试：解析优先级矩阵（static 命中不触碰后续路径；managed 命中不触碰 OIDC；无配置逐位回退）、吊销后立即 401、过期 401、禁用用户 401、错误 token 401、存储故障 401、volatile 模式 503/回退

### 进度注记（2026-11-01，存储/领域/认证解析落地）

- 已完成：`cp_users`/`cp_credentials`/`cp_sessions` 三表 + schema v5（SQLite/PG 双后端、部分唯一索引 `cp_users_oidc_subject`、备份 `TABLE_ORDER` 增用户与凭证、会话有意排除）；`hub/identity.rs` 用户目录（创建/更新/删除/最后 Admin 不变量/OIDC 绑定唯一/凭证上限）、`ark_<credential_id>_<secret>` 令牌（SHA-256(credential_id‖0x00‖secret)、常量时间比较、一次性明文、签发/轮换/吊销）、`revoke_user_tokens` epoch、托管凭证认证解析并接入 `operator_principal`（静态优先、托管次之、ark_ 前缀不回落 OIDC、失败 401）；审计漏斗与 `identity_store_unavailable` 等错误码；存储/领域/认证单测 8 个（含双后端 CRUD、唯一约束、级联、令牌篡改/吊销/过期/禁用、最后 Admin、`user_limit` 分支未单测）。
- 待办：会话持久化接入 OIDC 登录/登出与失效清扫；JWT `iat`/epoch 吊销判定；管理 API + `ManageUsers` 动作 + principal echo；Console 用户/凭证面板与运行期 token；en/zh 文档；备份行数显式断言、`PATCH` 空 patch 等边界；审计 actor 参数化。

## 4. 会话持久化

- [x] 4.1 `crates/arkflow-server/src/oidc.rs`：会话存储抽象（有存储走 `cp_sessions`，无存储保留内存实现）；`create_session/resolve_session/remove_session` 适配；cookie 形状与 `Max-Age/Secure` 派生逐字节不变（`api/diagnostics.rs:354-370`）
- [x] 4.2 登录/回调/登出：写/查/删持久会话；`session_ttl` 仍 8h；登出删行；`MAX_SESSIONS=4096` 先清扫后拒绝新登录（503 `session_capacity`）
- [x] 4.3 过期清扫：接入既有 maintenance sweep（60s 周期），单测过期行回收；测试：重启 Hub 后会话仍有效（持久库）、restore 后会话为空（不复活）
- [x] 4.4 会话吊销：`revoke-tokens` 删除指定 user 的全部会话；会话行带 `user_id`（未绑定 OIDC 主体为空）；测试：吊销后旧 cookie 立即 401

## 5. OIDC JWT 主体吊销

- [x] 5.1 `crates/arkflow-server/src/oidc.rs`：`sub` 绑定目录用户时以目录 role/scopes 为准，校验 `disabled` 与 `iat >= tokens_valid_after_ms - 60s`（缺 `iat` 且已绑定 → 401）；未绑定保持 claims 映射；`ARKFLOW_OIDC_MANAGED_USERS_ONLY`（默认 false，true 时未绑定 → 401）
- [x] 5.2 单测：绑定用户的角色取自目录（IdP claims 不覆盖）、disabled 401、epoch 前后边界、时钟容差、缺 iat 401、未绑定保持既有行为、`managed_users_only` 开关两态
- [x] 5.3 集成：`revoke-tokens` 后旧 JWT 立即 401；静态凭据与 OIDC bearer 既有优先级测试全部保持绿

### 进度注记（2026-11-01，会话与 JWT 吊销落地）

- 已完成：会话持久化（`Hub::create_session/resolve_session/remove_session`：有存储走 `cp_sessions` 哈希行、无存储回退内存；登录回调/登出/维护清扫接线；用户禁用/删除/revoke-tokens 会话立即失效；跨 Hub 重启可用）；JWT 主体吊销（`apply_federated_directory`：`oidc_subject` 绑定目录角色/scope、禁用 401、`iat` 早于 `tokens_valid_after_ms`−60s 401、绑定但缺 `iat` 401、未绑定按 claims 映射、`ARKFLOW_OIDC_MANAGED_USERS_ONLY` 默认 false）；`authenticate_with_iat` 贯通；维护 tick 增加 `prune_identity_sessions`；identity 测试扩到 9 个（含会话跨 Hub 重启、吊销后 cookie 失效、禁用用户会话失效、JWT epoch/禁用/managed-only 判定）。
- 待办：管理 API（用户/凭证/revoke-tokens + `ManageUsers`）+ principal echo；身份认证指标与审计拒绝路径补全；Console 用户面板/运行期 token；en/zh 文档；备份行数显式断言。

## 6. 管理 API

- [x] 6.1 `crates/arkflow-server/src/api/users.rs`（新模块）+ `api/mod.rs` 路由：用户 CRUD、凭证列表/签发/轮换/吊销、`revoke-tokens`；全部经 `require_operator_action(ManageUsers, "user", Some(id))`；401/403 与审计沿用既有漏斗
- [x] 6.2 响应与错误：列表分页（page/page_size 默认 50/上限 200）、秘密仅签发/轮换响应出现一次、错误码 `identity_store_unavailable`/`invalid_user`/`invalid_credential`/`user_not_found`/`credential_not_found`/`last_admin_required`/`credential_limit`/`user_limit`/`oidc_subject_conflict`
- [x] 6.3 `api_contract.rs`：新增 `ManageUsers` 动作（Admin 仅）并同步 `ALL`；DTO（`CreateUserRequest`/`UpdateUserRequest`/`CreateCredentialRequest`/`CredentialResponse` 等）；wire-shape 单测
- [x] 6.4 认证/授权矩阵测试（模板 `api/webhooks_api_tests.rs:49`）：无凭据 401、viewer/operator 403、admin 200；`principal_echo_api_tests.rs` 补 `manage_users` 口径（viewer/operator 无、admin 有）
- [x] 6.5 秘密泄漏测试：断言列表/详情/审计/日志/tracing 输出不含 token 或哈希（grep 响应体与审计 message）；401 不区分「不存在/错误」

### 进度注记（2026-11-01，管理 API 落地）

- 已完成：`ManageUsers` 动作（Admin 仅，principal echo 同步）；用户/凭证管理路由（列表/创建/详情/PATCH/删除、凭证列表/签发/轮换/吊销、revoke-tokens）；DTO 与错误码映射（user_not_found/credential_not_found/invalid_user/invalid_credential/last_admin_required/user_limit/credential_limit/oidc_subject_conflict/identity_store_unavailable）；一次性明文响应与元数据脱敏；API 测试 4 个（目录+凭证 CRUD、轮换/吊销、403 矩阵、最后 Admin 与 volatile 503）。
- 待办：身份认证指标（`arkflow_operator_auth_total`）与审计拒绝路径细化；Console 面板（delegated console-credentials）；en/zh 文档（delegated docs-credentials）；备份行数显式断言；`user_limit` 等边界 API 测试。

## 7. 可观测

- [x] 7.1 `metrics.rs`：`arkflow_operator_auth_total{source,outcome}`（source=static/managed/oidc/session/anonymous，outcome=ok/denied）与 `arkflow_identity_changes_total{kind}` 可选；低基数封闭标签测试
- [x] 7.2 审计事件全量：九种动作接受/拒绝均落 `cp_audit_events`，actor 为真实 principal，message 仅标量；测试断言 actor/action/outcome/failure_code

### 进度注记（2026-11-01，可观测与 Console 落地）

- 已完成：`arkflow_operator_auth_total{source,outcome}`（static/managed/oidc/session/anonymous；operator_principal 各分支埋点，含 managed/session 拒绝）；身份变更审计含拒绝路径（`record_identity_rejection` + `audited()`，failure_code=稳定码）；Console 用户/凭证独立 `/users` 页 + Settings/401 运行期 Bearer token（sessionStorage）+ en/zh 文案（delegated，106 tests 通过）；credentials en/zh 文档（delegated，docs:check 145 页通过）。
- 待办：备份行数显式断言、`user_limit`/空 patch 等边界 API 测试、`GET /users/{id}/credentials` 404 语义的文档同步（已通知 docs）、pause 收尾测试与两份 change 的归档。

## 8. Console

- [x] 8.1 `console/src/api.ts` + `features/types.ts`：用户/凭证 API 客户端；`OperatorAction` TS 联合类型加 `manage_users`；`JobRecord` 无涉及
- [x] 8.2 运行期凭证输入：401 时若 OIDC 登录流不可用则弹凭证输入；Bearer token 存 `sessionStorage`、可清除、可切换身份；`VITE_API_TOKEN` 降级为可选默认值；测试覆盖输入后重试与清除
- [x] 8.3 用户与凭证面板（`features/settings.tsx` 内或新 `features/users.tsx`）：列表/创建/编辑角色与 scope/禁用/删除；凭证签发（一次性展示+复制）/轮换/吊销/吊销全部令牌；`can('manage_users')` 门控 + Hub-only + 确认组件；错误码展示
- [x] 8.4 i18n en/zh 全量；改写 `settings.credentialsNote`（en.ts:208-209 / zh.ts:199-200）；`pnpm -C console test`、lint、typecheck 绿

## 9. 文档

- [x] 9.1 新增 `docs/docs/operate/control-plane/users-and-credentials.md`：用户/角色/scope 概念、签发/轮换/吊销流程、bootstrap 与 break-glass、HA/restore 语义（会话失效）、密级提示
- [x] 9.2 更新 `docs/docs/operate/control-plane/deploy.md`：删除「there is no revocation list」两处（:331、:343），补吊销语义与管理 API 开关；`docs/docs/reference/api.md`：用户/凭证端点表、权限、401/403 语义、一次性秘密；`docs/docs/reference/configuration.md`（env 清单、`ARKFLOW_OIDC_MANAGED_USERS_ONLY`）；`docs/docs/operate/control-plane/console.md`（运行期凭证输入与身份切换）；`docs/docs/operate/helm.md` + chart 如需注入配置
- [x] 9.3 zh-Hans 对应全部页面同步（含新增页 `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/operate/control-plane/users-and-credentials.md`）
- [x] 9.4 `SECURITY.md` 凭证保管/吊销/break-glass 语义；`CHANGELOG.md` 条目；`pnpm docs:check` 绿

### 进度注记（2026-11-01，边界与备份断言落地）

- 已完成：`tests/backup_restore.rs` 新增身份目录备份断言（cp_users/cp_credentials 随快照往返、cp_sessions 不复活、快照只含凭证哈希）；API 边界测试（空 PATCH → 400 invalid_user、未知用户列凭证 → 404 user_not_found、第 1025 个用户 → 409 user_limit）。

## 10. 验证与归档

- [x] 10.1 `cargo test -p arkflow-server`、`cargo test -p arkflow-core`、clippy、fmt 全绿；PG 门控套件在真库上跑通
- [x] 10.2 端到端：本地 SQLite Hub → 创建第二用户并签发凭证 → 该凭证按角色成功/被拒 → 吊销后立即 401 → 浏览器会话被吊销 → 重启 Hub 会话仍在 → backup/restore 后用户与凭证在、会话不在
- [ ] 10.3 HA 一致性：同一 PG 上 leader 吊销后（模拟 standby 提升）新 leader 下旧凭证仍 401；stale epoch 写被拒
- [x] 10.4 `openspec validate add-multi-user-credential-management --strict`；与 `add-job-pause-resume` 一并归档后 `openspec/changes/` 清空


## 归档说明（2026-11-01）

- 完成情况：schema v5、用户目录、托管凭证、会话持久化、JWT 主体吊销、管理 API + `ManageUsers`、Console `/users` 面板 + 运行期 token、en/zh 文档与备份/边界测试全部落地；`cargo test -p arkflow-server --all-targets` 绿（509 lib + 全部集成套件），`pnpm -C console test` 106 绿，`pnpm -C docs docs:check` 145 页绿。
- 10.3（HA 下吊销跨 leader 的 PG 端到端）未执行：需要真实 PostgreSQL 双实例环境；吊销状态与读路径均走共享存储（无缓存，语义上跨 leader 立即生效），以门控 PG 契约测试与持久化测试覆盖。
- 已记录但未在本变更内修复的既有缺口：`ReadAudit` 从未强制、JSON `/audit` 过滤能力不足（见 proposal Non-goals）；身份审计的 operations-level actor（JobStart/Stop）参数化另立专项。
- 环境说明：`cargo test --workspace --all-targets` 在 macOS 沙箱下因 `libduckdb-sys` C++ 编译错误（`FP_NAN` undeclared）失败，属环境/工具链问题；受影响 crate 的测试在 `arkflow-server`/`arkflow-core` 层面全部通过。
