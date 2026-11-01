# Delta Spec: hub-oidc-auth

## MODIFIED Requirements

### Requirement: 会话参与授权解析

授权解析 SHALL 在无 Authorization 头时回退读取 `arkflow_session` cookie：会话 id 有效（存在、未过期且未被吊销）时 SHALL 以其创建时的 principal 参与既有 RBAC 授权；无效、过期或已吊销 SHALL 按未认证处理。静态凭据与 OIDC bearer 路径 SHALL 保持既有优先级与语义。在有 durable storage 的 Hub 上，会话 SHALL 持久化在 `cp_sessions`（随机会话 id、principal 快照、可选绑定 user id、过期时间与吊销时间），会话 cookie 的形态、属性与 TTL（8h）SHALL 保持不变；会话 SHALL 跨 Hub 重启与选主存活，SHALL 在其绑定用户被禁用、删除或吊销令牌后立即失效，且 control-plane 备份/恢复 MUST NOT 复活会话。在无 durable storage 的 Hub 上 MAY 使用进程内会话，但接口与文档 SHALL 声明该模式下会话不可被吊销。

#### Scenario: 会话 cookie 通过授权

- **WHEN** 已登录用户的浏览器请求（携带有效 `arkflow_session` cookie）访问只读 API
- **THEN** 以创建会话时的 principal（角色/scope）通过授权

#### Scenario: 过期或伪造会话按未认证处理

- **WHEN** 会话 id 不存在、已过期或被伪造
- **THEN** 请求返回 401

#### Scenario: 会话跨 Hub 重启存活

- **WHEN** 浏览器登录后 Hub 以同一 durable storage 重启
- **THEN** 既有会话 cookie 在其过期前仍通过认证

#### Scenario: 用户吊销后会话立即失效

- **WHEN** 管理员禁用某用户或吊销其全部令牌
- **THEN** 该用户的既有会话 cookie 在下一个请求即返回 401

#### Scenario: 备份恢复不复活会话

- **WHEN** 控制面快照被恢复到空库
- **THEN** 用户与凭证记录存在，而任何浏览器会话都不存在

## ADDED Requirements

### Requirement: Managed OIDC subjects SHALL be revocable

当 OIDC bearer 令牌的 `sub` 绑定到托管用户（`cp_users.oidc_subject`）时，Hub SHALL 以目录中的角色与 scope 构造 principal（IdP 的 role/scopes claims 不再覆盖目录），并 SHALL 在该用户被禁用、或其令牌 `iat` 早于该用户的 `tokens_valid_after_ms`（允许 60 秒时钟偏移）时以 401 拒绝；已绑定用户但令牌缺少 `iat` SHALL 拒绝。`sub` 未绑定时 SHALL 保持既有 claims 映射语义。配置 `ARKFLOW_OIDC_MANAGED_USERS_ONLY=true` 时，未绑定的主体 SHALL 一律 401；默认 false SHALL 与现状逐字节一致。现有的 JWT 校验失败语义（签名、`exp`、`iss`、`aud`、算法白名单、`sub` 缺失、角色无匹配）SHALL 保持不变。

#### Scenario: 目录角色覆盖 IdP claims

- **WHEN** 绑定了目录用户的令牌携带与目录不同的 role/scopes claims
- **THEN** 授权使用目录中的角色与 scope

#### Scenario: 禁用用户的令牌被拒

- **WHEN** 已绑定目录用户被禁用后其令牌仍被提交
- **THEN** 请求返回 401

#### Scenario: 吊销阈值生效

- **WHEN** 管理员吊销用户令牌后，该用户一个 `iat` 早于吊销阈值的令牌被提交
- **THEN** 请求返回 401，且无需 IdP 配合

#### Scenario: 未绑定主体保持既有行为

- **WHEN** `ARKFLOW_OIDC_MANAGED_USERS_ONLY` 未启用且令牌 `sub` 未绑定任何目录用户
- **THEN** 令牌按既有 claims 映射为 principal，行为与变更前一致

#### Scenario: 缺 iat 的已绑定令牌被拒

- **WHEN** 一个已绑定目录用户的令牌不包含 `iat` claim
- **THEN** 请求返回 401
