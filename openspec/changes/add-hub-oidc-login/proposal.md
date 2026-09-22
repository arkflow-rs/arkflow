## Why

`hub-oidc-auth` 已让 Hub 接受 IdP 签发的 JWT bearer（面向 API 客户端/自动化），但**人类用户无法从浏览器登录 Console**——没有授权码流，用户仍需手工获取并配置一个短时令牌，这违背 SSO 的初衷。企业部署里「人员经 IdP 登录控制台」是 OIDC 集成的主要用例。全部依赖已在位（reqwest/jsonwebtoken/rand），OidcAuthenticator 已能验证 id_token（签名/iss/aud/exp），本变更只加「浏览器授权码交换 + 服务端会话」一层。

## What Changes

- `crates/arkflow-server/src/oidc.rs` 扩展：
  - `OidcFederation`（持有 OidcAuthenticator + 可选 client + 会话表）：`from_env()` 异步装配——issuer/audience 必填（与 bearer 模式一致）；`ARKFLOW_OIDC_CLIENT_ID`/`ARKFLOW_OIDC_CLIENT_SECRET`/`ARKFLOW_OIDC_REDIRECT_URI` 三者齐备时启用登录流；启动时经 discovery（`{issuer}/.well-known/openid-configuration`）获取 authorization_endpoint/token_endpoint，失败则告警并降级为仅 bearer；
  - 内存会话表：随机会话 id（OsRng）→ `OperatorPrincipal`，固定 8h TTL，惰性清理；
  - `create_session`/`resolve_session` API。
- Hub 新增两个免认证路由（仅登录流启用时注册）：
  - `GET /api/v1/auth/oidc/login`：生成随机 state（OsRng）、写 `arkflow_oidc_state` HttpOnly cookie、302 到 IdP 授权端点；
  - `GET /api/v1/auth/oidc/callback`：校验 state cookie → code 换 id_token（form POST）→ OidcAuthenticator 验证 → 建会话 → 写 `arkflow_session` HttpOnly cookie → 303 回 Console 根路径；state 不匹配或交换失败返回 401；
  - `GET /api/v1/auth/oidc/logout`：删除服务端会话 + 清除 cookie。
- 授权桥接：`bearer()` 在无 Authorization 头时回退读取 `arkflow_session` cookie（前缀 `session:`），`operator_principal` 先解析会话再走静态凭据/OIDC bearer 路径——既有授权语义（RBAC/审计）零变化。
- 新增 env：`ARKFLOW_OIDC_CLIENT_ID`/`ARKFLOW_OIDC_CLIENT_SECRET`/`ARKFLOW_OIDC_REDIRECT_URI`。

## Capabilities

### New Capabilities

<!-- 无新能力：本变更扩展 `hub-oidc-auth`。 -->

### Modified Capabilities

- `hub-oidc-auth`: 新增浏览器授权码登录流（login/callback/logout 端点、state cookie、会话表、cookie 会话参与授权解析）的需求。

## Impact

- `crates/arkflow-server/src/oidc.rs`：federation/discovery/会话表；`hub.rs`：路由注册 + 两个 handler + 会话解析分支；`lib.rs`：`bearer()` 回退 cookie、路由挂载。
- 无新增依赖（rand/reqwest/jsonwebtoken 已在）。
- 文档：控制面部署页 OIDC 节扩展（登录流环境变量与会话语义，en/zh）。
- 测试：mock IdP（discovery/token/JWKS 三端点）全链路：login 重定向与 state cookie、callback 换取会话、会话参与授权、state 不匹配拒绝、logout、未启用时路由不存在。

## Non-goals

- 不做 PKCE（v1 用 state cookie + server-side client_secret；IdP 侧策略需要时后补）。
- 不做持久化会话/吊销列表（重启清空，8h TTL 兜底）；不做 refresh token。
- 不改 Console 前端（浏览器访问 `/auth/oidc/login` 即完成登录；cookie 自动随请求携带）。
- 不做 OIDC discovery 的动态 client 注册、多 IdP 并存。
