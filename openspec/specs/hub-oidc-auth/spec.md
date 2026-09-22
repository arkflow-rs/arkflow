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

签名验证 SHALL 基于 JWKS 端点公钥（按令牌 `kid` 选钥）；JWKS SHALL 带 TTL 缓存，`kid` 未命中时 SHALL 强制刷新一次后重试，仍未命中 SHALL 拒绝；缓存中的已知 `kid` SHALL 在 TTL 过期后仍可用于验证（避免可用性悬崖）。以下令牌 SHALL 一律解析失败（401，与未授权语义一致）：签名无效、`exp` 过期、`iss` 不匹配、`aud` 不匹配、算法在白名单外、`sub` 缺失、角色无匹配。

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

### Requirement: 装配与零配置兼容

OIDC SHALL 经环境变量装配：`ARKFLOW_OIDC_ISSUER` 与 `ARKFLOW_OIDC_AUDIENCE` 同时存在即启用；`ARKFLOW_OIDC_JWKS_URL`（缺省 `{issuer}/.well-known/jwks.json`）、`ARKFLOW_OIDC_ROLE_CLAIM`、`ARKFLOW_OIDC_SCOPES_CLAIM` 可覆盖。未配置时 SHALL 不构造验证器、不发起任何 JWKS 请求。既有测试与部署（静态凭据、insecure local 模式）SHALL 不受影响。

#### Scenario: 未配置零行为变化

- **WHEN** 未设置任何 OIDC 环境变量
- **THEN** Hub 构造不包含验证器，JWT bearer 被按现状拒绝，静态凭据路径不变

#### Scenario: 自定义 claim 名

- **WHEN** 配置 `ARKFLOW_OIDC_ROLE_CLAIM=groups` 且令牌 `groups: ["viewer"]`
- **THEN** 主角色解析为 viewer
