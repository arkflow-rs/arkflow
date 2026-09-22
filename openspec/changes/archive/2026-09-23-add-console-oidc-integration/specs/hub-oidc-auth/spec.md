# hub-oidc-auth 变更（Delta）

## ADDED Requirements

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
