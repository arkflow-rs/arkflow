# hub-oidc-login 变更（Delta）

## MODIFIED Requirements

### Requirement: 装配与零配置兼容

OIDC SHALL 经环境变量装配：`ARKFLOW_OIDC_ISSUER` 与 `ARKFLOW_OIDC_AUDIENCE` 同时存在即启用 bearer 验证；`ARKFLOW_OIDC_JWKS_URL`（缺省 `{issuer}/.well-known/jwks.json`）、`ARKFLOW_OIDC_ROLE_CLAIM`、`ARKFLOW_OIDC_SCOPES_CLAIM` 可覆盖 claim 名。当 `ARKFLOW_OIDC_CLIENT_ID`、`ARKFLOW_OIDC_CLIENT_SECRET` 与 `ARKFLOW_OIDC_REDIRECT_URI` 三者同时提供时 SHALL 额外启用浏览器登录流并执行一次 OIDC discovery（获取 authorization/token endpoint，失败则告警并降级为仅 bearer 模式）。未配置任何 OIDC 环境变量时 SHALL 不构造验证器、不发起任何网络请求。

#### Scenario: 仅 bearer 模式（缺 client 凭据）

- **WHEN** 仅配置 issuer/audience
- **THEN** JWT bearer 验证生效，登录路由不注册，行为与既有 bearer 模式一致

#### Scenario: 完整配置启用登录流

- **WHEN** 配置 issuer/audience/client_id/client_secret/redirect_uri 且 IdP discovery 可达
- **THEN** 登录路由注册成功，discovery 得到 authorization/token endpoint

#### Scenario: discovery 失败降级

- **WHEN** client 凭据齐备但 IdP discovery 不可达
- **THEN** 启动继续、告警说明降级为仅 bearer 模式

## ADDED Requirements

### Requirement: 浏览器授权码登录流

登录流启用时，Hub SHALL 提供 `GET /auth/oidc/login`（生成随机 state、写 `arkflow_oidc_state` HttpOnly cookie、302 到 IdP 授权端点）、`GET /auth/oidc/callback`（校验 state → code 换 id_token → 按既有 hub-oidc-auth 验证并映射 principal → 创建 8h 内存会话 → 写 `arkflow_session` HttpOnly cookie → 303 到 `/`）与 `GET /auth/oidc/logout`（删除服务端会话并清除 cookie）。state 不匹配、code 交换失败或 id_token 验证失败 SHALL 返回 401。

#### Scenario: login 重定向携带 state

- **WHEN** 用户访问 `/auth/oidc/login`
- **THEN** 响应为 302 到 IdP 授权端点（含 client_id、redirect_uri、`response_type=code`、`scope=openid` 与随机 `state`），并写入 HttpOnly state cookie

#### Scenario: callback 完成登录并建立会话

- **WHEN** IdP 重定向回 callback（code + 匹配的 state）
- **THEN** Hub 用 code 换取 id_token，验证后创建 8h 会话，写 `arkflow_session` HttpOnly cookie 并 303 到 `/`

#### Scenario: state 不匹配拒绝

- **WHEN** callback 的 state 与 cookie 不一致
- **THEN** 返回 401，不建立会话

#### Scenario: logout 清除会话

- **WHEN** 持有效会话的用户访问 `/auth/oidc/logout`
- **THEN** 服务端会话被删除、cookie 被清除，后续请求不再认证

### Requirement: 会话参与授权解析

授权解析 SHALL 在无 Authorization 头时回退读取 `arkflow_session` cookie：会话 id 有效（存在且未过 8h TTL）时 SHALL 以其创建时的 principal 参与既有 RBAC 授权；无效或过期 SHALL 按未认证处理。静态凭据与 OIDC bearer 路径 SHALL 保持既有优先级与语义。

#### Scenario: 会话 cookie 通过授权

- **WHEN** 已登录用户的浏览器请求（携带有效 `arkflow_session` cookie）访问只读 API
- **THEN** 以创建会话时的 principal（角色/scope）通过授权

#### Scenario: 过期或伪造会话按未认证处理

- **WHEN** 会话 id 不存在、已过期或被伪造
- **THEN** 请求返回 401
