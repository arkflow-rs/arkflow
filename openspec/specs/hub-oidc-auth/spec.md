# Capability: Hub OIDC Auth

## Purpose

Define OIDC JWT bearer federation for Hub operator APIs: token validation semantics (asymmetric algorithms, signature, issuer, audience, expiry), claims-to-principal mapping onto the existing role/scope RBAC model, JWKS caching and refresh, and coexistence with static operator credentials. (Purpose derived from the `add-hub-oidc-auth` change; refine as the capability evolves.)

## Requirements

### Requirement: OIDC JWT bearer 主解析

配置 OIDC 后（issuer 与 audience 同时提供），Hub SHALL 接受 IdP 签发的 JWT bearer 作为 operator 请求的身份来源：验证签名（仅允许非对称算法 ES256/RS256，拒绝 HS256 与 `none`）、`exp`、`iss` 与 `aud` 后，SHALL 把 claims 映射为既有 `OperatorPrincipal`——`sub` 为主 id；`role_claim`（默认 `roles`，数组或单字符串，大小写不敏感）按 admin > operator > viewer 取最高匹配角色，无匹配角色 SHALL 拒绝；`scopes_claim`（默认 `scopes`，数组或逗号分隔）按既有 `type=id` 文法解析为资源 scope。静态凭据 SHALL 优先于 OIDC：配置的静态凭据命中时不触碰 OIDC 路径。未配置 OIDC 时行为 SHALL 与现状逐字节一致。

#### Scenario: 有效令牌映射为 viewer 主

- **WHEN** 请求携带 IdP 签发的有效 JWT（`sub: u1`、`roles: ["viewer"]`）且 OIDC 配置了匹配的 issuer/audience
- **THEN** 该请求以 id `u1`、角色 viewer 的主通过授权，读操作成功、变更操作被拒（沿用既有 RBAC）

#### Scenario: 多角色取最高权限

- **WHEN** 令牌 `roles: ["viewer", "operator"]`
- **THEN** 主角色为 operator

#### Scenario: 静态凭据优先

- **WHEN** bearer 与配置的静态凭据匹配
- **THEN** 直接按静态凭据文法解析主，不发起 JWKS 验证

### Requirement: 令牌验证与失败语义

签名验证 SHALL 基于 JWKS 端点公钥（按令牌 `kid` 选钥）；JWKS SHALL 带 TTL 缓存，`kid` 未命中时 SHALL 强制刷新一次后重试，仍未命中 SHALL 拒绝；缓存中的已知 `kid` SHALL 在 TTL 过期后仍可用于验证（避免可用性悬崖）。JWKS 刷新 SHALL NOT 在持有缓存锁的状态下进行网络请求：缓存命中 SHALL 在锁内即时返回，不受并发刷新影响；并发触发的重复刷新 SHALL 去重：未持有刷新权的一方 SHALL 等待在途刷新完成后重查缓存一次，而不是直接拒绝可能携带新轮换密钥的有效令牌。以下令牌 SHALL 一律解析失败（401，与未授权语义一致）：签名无效、`exp` 过期、`iss` 不匹配、`aud` 不匹配、算法在白名单外、`sub` 缺失、角色无匹配。

#### Scenario: 过期令牌拒绝

- **WHEN** 令牌 `exp` 早于当前时间
- **THEN** 请求返回 401，行为与未认证一致

#### Scenario: 算法混淆拒绝

- **WHEN** 令牌 header 声明 `alg: HS256` 或 `none`
- **THEN** 令牌被拒绝，不尝试 JWKS 验证

#### Scenario: kid 未命中触发刷新

- **WHEN** 令牌 `kid` 不在缓存中且 JWKS 端点已更新
- **THEN** 强制重新拉取 JWKS 一次，新 `kid` 的令牌验证成功

#### Scenario: 坏签名拒绝

- **WHEN** 令牌被其他密钥签名
- **THEN** 请求返回 401

#### Scenario: 并发未命中等待共享刷新

- **WHEN** 两个携带同一未知 `kid` 的请求并发到达且该 `kid` 已在 JWKS 端点发布
- **THEN** 只发起一次 JWKS 刷新，两个请求都在刷新完成后验证成功

#### Scenario: 刷新期间缓存命中不被阻塞

- **WHEN** 一个未知 `kid` 触发 JWKS 刷新（网络请求进行中），另一请求携带缓存中已知 `kid` 的令牌到达
- **THEN** 已知 `kid` 的验证即时完成，不等待刷新的网络请求返回

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

### Requirement: 浏览器授权码登录流

登录流启用时，Hub SHALL 提供 `GET /auth/oidc/login`（生成随机 state、PKCE `code_verifier` 与 `nonce`，写携带三元组的 `arkflow_oidc_state` HttpOnly cookie，302 到 IdP 授权端点并在查询中携带 `code_challenge`（S256）、`code_challenge_method=S256`、`nonce`）、`GET /auth/oidc/callback`（校验 state → 以 `code_verifier` 换 id_token → 校验 id_token 的 `nonce` claim → 按既有 hub-oidc-auth 验证并映射 principal → 创建 8h 内存会话 → 写 `arkflow_session` HttpOnly cookie → 303 到 `/`）与 `GET /auth/oidc/logout`（删除服务端会话并清除 cookie）。state 不匹配、code 交换失败、nonce claim 不匹配或 id_token 验证失败 SHALL 返回 401。`arkflow_session` cookie 的 `Max-Age` SHALL 从会话 TTL 派生；redirect URI 以 `https://` 开头时 SHALL 附加 `Secure` 属性。登录流启动或会话创建所需的 OS 随机性不可用时 SHALL 返回 503/500 错误响应，SHALL NOT panic。

#### Scenario: login 重定向携带 state

- **WHEN** 用户访问 `/auth/oidc/login`
- **THEN** 响应为 302 到 IdP 授权端点（含 client_id、redirect_uri、`response_type=code`、`scope=openid`、随机 `state`、`code_challenge` 与 `nonce`），并写入 HttpOnly state cookie

#### Scenario: callback 完成登录并建立会话

- **WHEN** IdP 重定向回 callback（code + 匹配的 state），Hub 以 code_verifier 换取 id_token，其 nonce claim 与 cookie 中值一致且签名/iss/aud 验证通过
- **THEN** Hub 创建 8h 会话，写 `arkflow_session` HttpOnly cookie 并 303 到 `/`

#### Scenario: nonce 不匹配拒绝

- **WHEN** callback 收到的 id_token 的 nonce claim 与 cookie 中存放的值不一致
- **THEN** 返回 401，不建立会话

#### Scenario: state 不匹配拒绝

- **WHEN** callback 的 state 与 cookie 不一致
- **THEN** 返回 401，不建立会话

#### Scenario: logout 清除会话

- **WHEN** 持有效会话的用户访问 `/auth/oidc/logout`
- **THEN** 服务端会话被删除、cookie 被清除，后续请求不再认证

#### Scenario: https 部署的会话 cookie 附加 Secure

- **WHEN** 登录流配置的 redirect URI 以 `https://` 开头且 callback 成功建立会话
- **THEN** `arkflow_session` Set-Cookie 携带 `Secure` 属性且 `Max-Age` 等于会话 TTL 秒数

#### Scenario: 随机性不可用返回错误而非崩溃

- **WHEN** login 处理器生成 state 时 OS 随机源不可用
- **THEN** 返回 503 错误响应，进程不 panic

### Requirement: 会话参与授权解析

授权解析 SHALL 在无 Authorization 头时回退读取 `arkflow_session` cookie：会话 id 有效（存在且未过 8h TTL）时 SHALL 以其创建时的 principal 参与既有 RBAC 授权；无效或过期 SHALL 按未认证处理。静态凭据与 OIDC bearer 路径 SHALL 保持既有优先级与语义。

#### Scenario: 会话 cookie 通过授权

- **WHEN** 已登录用户的浏览器请求（携带有效 `arkflow_session` cookie）访问只读 API
- **THEN** 以创建会话时的 principal（角色/scope）通过授权

#### Scenario: 过期或伪造会话按未认证处理

- **WHEN** 会话 id 不存在、已过期或被伪造
- **THEN** 请求返回 401

### Requirement: OIDC 状态探测端点

Hub SHALL 提供免认证的 `GET /api/v1/auth/oidc/status` 端点（登录流未启用时也 SHALL 注册），返回 JSON：`login_enabled`（登录流是否启用）、`authenticated`（会话 cookie 是否解析出有效 principal）、`principal`（有效时含 `id` 与 `roles`，否则 null）。端点 SHALL NOT 泄漏凭据或会话 id。

#### Scenario: 登录流启用且持有效会话

- **WHEN** 持有效 `arkflow_session` cookie 请求 status
- **THEN** 返回 `login_enabled: true`、`authenticated: true` 且 `principal.id` 为会话主 id、`roles` 为其角色

#### Scenario: 未认证探测

- **WHEN** 无会话 cookie 请求 status
- **THEN** 返回 `login_enabled` 与 `authenticated: false`、`principal: null`（HTTP 200，供 Console 探测）

#### Scenario: 登录流未启用

- **WHEN** 未配置 OIDC client 凭据时请求 status
- **THEN** 返回 `login_enabled: false`

### Requirement: JWKS SHALL refresh periodically and revoke absent kids
Hub SHALL 按可配置间隔（默认 1 小时）周期性整表刷新 JWKS 缓存；刷新成功后，不在新表中的 known kid SHALL 自然吊销（后续校验按未知 kid 处理并 401）。刷新失败时 SHALL 保留旧表继续服务（provider 故障 MUST NOT 打掉 Hub）并记录 warn 日志；既有「未知 kid 触发即时刷新（节流保留）」语义保持不变。

#### Scenario: 密钥轮换后旧 kid 被吊销
- **WHEN** IdP 轮换签名密钥且 Hub 的周期刷新成功拉取到不含旧 kid 的新 JWKS
- **THEN** 旧 kid 签发的令牌在校验时按未知 kid 处理（触发即时刷新后仍不存在则 401）

#### Scenario: 刷新失败不打掉 Hub
- **WHEN** 周期刷新时 IdP 暂时不可达
- **THEN** Hub 保留旧 JWKS 表继续校验既有 kid，并记录刷新失败 warn 日志

#### Scenario: 未配置 OIDC 时零行为变化
- **WHEN** Hub 未启用 OIDC 认证
- **THEN** 不启动周期刷新任务，行为与既有零配置兼容语义一致
