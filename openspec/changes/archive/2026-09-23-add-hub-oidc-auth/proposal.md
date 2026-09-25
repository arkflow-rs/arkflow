## Why

`control-plane-identity` 能力已实现基于凭据文法的 RBAC（`hub.rs` 的 `parse_operator_credential`：`id|role|secret|type=id,...` 三角色 + 资源 scope + `operator_can_scope` 细粒度授权——PLANNING.md「剩余 RBAC/OIDC」中 RBAC 部分实际已完成，本文档一并修正）。**缺的是 OIDC 身份联邦**：企业环境中凭据不该由 Hub 静态配置，而应由组织 IdP（Okta/Entra/Keycloak）签发——运维无法为每个人员/服务轮换静态 token，也无法集中吊销。本变更让 Hub 作为资源服务器接受 OIDC IdP 签发的 JWT bearer：验证签名/签发者/受众/有效期后映射到既有 `OperatorPrincipal`（角色与资源 scope 走既有 RBAC 模型），静态凭据路径保持不变（渐进迁移）。

## What Changes

- 新增 `crates/arkflow-server/src/oidc.rs`：`OidcAuthenticator`
  - 从 JWKS 端点拉取公钥（TTL 缓存 + kid 未命中时刷新一次）；按 `kid` 选钥，算法仅允许非对称（ES256/RS256，显式拒绝 HS256/none——防止 JWKS 变共享密钥预言机）；
  - 验证签名、`exp`、`iss`、`aud` 后把 claims 映射为 `OperatorPrincipal`：`sub` → id；`role_claim`（默认 `roles`，数组或字符串）按 admin > operator > viewer 取最高匹配角色；`scopes_claim`（默认 `scopes`，数组或逗号分隔）复用既有 `type=id` scope 文法；
  - 环境变量装配：`ARKFLOW_OIDC_ISSUER` + `ARKFLOW_OIDC_AUDIENCE`（同时存在即启用）、可选 `ARKFLOW_OIDC_JWKS_URL`/`ARKFLOW_OIDC_ROLE_CLAIM`/`ARKFLOW_OIDC_SCOPES_CLAIM`。
- Hub 接线：`Hub` 持有可选 `OidcAuthenticator`（`Hub::with_oidc`）；`operator_principal` 变为 async 并在静态凭据不匹配时回退 OIDC 验证（`operator_authorized`/`operator_can`/`operator_can_scope` 同步异步化，调用点补 `.await`——全部位于 arkflow-server 的 async handler/测试内）。
- 新增依赖 `jsonwebtoken = "9"`（workspace 集中管理）。
- PLANNING.md 修正：RBAC 已实现（7.3-5 与 5.7 阶段 4 的表述更新）。

## Capabilities

### New Capabilities

- `hub-oidc-auth`: Hub 的 OIDC JWT bearer 身份联邦——令牌验证语义、claims → 主映射（角色/scope）、JWKS 缓存与刷新、失败语义、与静态凭据的共存规则。

### Modified Capabilities

<!-- `control-plane-identity` 的需求文本不变：OIDC 只是 `Authenticated operator principal` 的一种新解析来源，行为仍由既有 RBAC/审计需求约束。 -->

## Impact

- `crates/arkflow-server/src/hub.rs`：3 个鉴权方法异步化 + 约 24 个调用点补 `.await`（handler 与测试）；`Hub::with_oidc`。
- `crates/arkflow-server/src/lib.rs`：21 处 handler 授权调用补 `.await`。
- `crates/arkflow-server/src/bin/arkflow-server.rs` 与单进程装配点：env 装配 OIDC。
- `Cargo.toml`：workspace 新增 `jsonwebtoken = "9"`。
- 文档：控制面文档页新增 OIDC 节；PLANNING.md 修正与记录。

## Non-goals

- 不做 OIDC 授权码/浏览器重定向流（Hub 是资源服务器，只接受 bearer JWT；登录由 IdP/前端完成）。
- 不做用户/团队/租户持久化模型（principal 每请求解析，无状态）；不做 token 吊销列表（依赖 exp + 短 TTL，文档说明）。
- 不做 Console 的 OIDC 登录 UI。
- 不改动既有静态凭据文法与审计语义。
