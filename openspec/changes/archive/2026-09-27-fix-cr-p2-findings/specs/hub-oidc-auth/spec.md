## MODIFIED Requirements

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

## MODIFIED Requirements

### Requirement: 令牌验证与失败语义

签名验证 SHALL 基于 JWKS 端点公钥（按令牌 `kid` 选钥）；JWKS SHALL 带 TTL 缓存，`kid` 未命中时 SHALL 强制刷新一次后重试，仍未命中 SHALL 拒绝；缓存中的已知 `kid` SHALL 在 TTL 过期后仍可用于验证（避免可用性悬崖）。JWKS 刷新 SHALL NOT 在持有缓存锁的状态下进行网络请求：缓存命中 SHALL 在锁内即时返回，不受并发刷新影响；并发触发的重复刷新 SHALL 去重。以下令牌 SHALL 一律解析失败（401，与未授权语义一致）：签名无效、`exp` 过期、`iss` 不匹配、`aud` 不匹配、算法在白名单外、`sub` 缺失、角色无匹配。

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

#### Scenario: 刷新期间缓存命中不被阻塞

- **WHEN** 一个未知 `kid` 触发 JWKS 刷新（网络请求进行中），另一请求携带缓存中已知 `kid` 的令牌到达
- **THEN** 已知 `kid` 的验证即时完成，不等待刷新的网络请求返回
