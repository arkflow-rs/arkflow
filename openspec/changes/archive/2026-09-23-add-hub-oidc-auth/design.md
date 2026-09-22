## Context

Hub 现有授权：静态凭据文法（`id|role|secret|scopes`）→ `OperatorPrincipal` → `OperatorAction`/`ResourceScope` RBAC（`control-plane-identity` spec）。`operator_principal` 为同步方法；bearer 提取在 `lib.rs` 的 axum handler 内（全部 async）。授权入口集中在 `operator_principal`/`operator_authorized`/`operator_can`/`operator_can_scope` 四个方法（lib.rs 21 处、hub.rs 测试 10 处调用）。

约束：不阻塞 async 运行时（排除同步 HTTP/block_on）；算法白名单默认仅非对称；`jsonwebtoken = "9"` 为唯一新依赖（JWKS 解析含于其 `jwk` 模块）。

## Goals / Non-Goals

**Goals:**
- IdP 签发的 JWT bearer 与静态凭据并存，验证通过即映射为既有 principal（角色/scope 语义零改动）。
- JWKS 拉取带 TTL 缓存与 kid 未命中刷新；所有安全失败（签名/过期/受众/签发者/算法）一律返回 None → 401，与现有未授权语义一致。

**Non-Goals:** 授权码流、吊销列表、用户持久化、Console UI（见 proposal）。

## Decisions

1. **鉴权入口异步化而非后台刷新**：`operator_principal` 等 3 个包装方法改为 async，调用点补 `.await`。备选「后台任务定时刷 JWKS + 冷缓存拒真」——拒绝：启动窗口内有效令牌被拒是隐形故障面；备选同步 HTTP 客户端——新增依赖且阻塞运行时。异步化是 ~35 行机械改动，语义正确。
2. **算法白名单硬编码非对称（ES256/RS256）**，不接受配置覆盖为 HS256：JWKS 公钥端点若允许对称算法会退化为共享密钥预言机。`decode_header` 先取 alg/kid，白名单外直接拒绝（防 `alg=none`/算法混淆）。
3. **JWKS 缓存**：`tokio::sync::Mutex<Option<Cached>>`（keys 按 kid 索引的 `DecodingKey` + 拉取时间），TTL 10 分钟；**kid 未命中且缓存未过期时强制刷新一次**（IdP 轮换密钥的常见时序），刷新后仍未命中才拒绝。并发拉取去重由 Mutex 天然串行化。
4. **claims 映射**：`sub` → principal id（缺失 → None）；`role_claim`（默认 `roles`，接受 JSON 数组或单字符串，大小写不敏感）按 admin > operator > viewer 取**最高**匹配角色（用户属于多组时取高权限，与多数 IdP 集成惯例一致）；无匹配角色 → None（401 而非降权 viewer——宁可显式失败）。`scopes_claim`（默认 `scopes`，数组或逗号分隔字符串）复用 `type=id` 文法解析为 `ResourceScope`。
5. **验证参数**：`iss` 必须等于配置 issuer；`aud` 必须包含配置 audience；`exp`/`nbf` 由 jsonwebtoken 标准校验（默认 leeway 60s 不调）。
6. **装配**：`OidcAuthenticator::from_env()`——`ARKFLOW_OIDC_ISSUER` + `ARKFLOW_OIDC_AUDIENCE` 同时存在即启用；`ARKFLOW_OIDC_JWKS_URL` 缺省为 `{issuer}/.well-known/jwks.json`；`ARKFLOW_OIDC_ROLE_CLAIM`/`ARKFLOW_OIDC_SCOPES_CLAIM` 可覆盖 claim 名。standalone server 与单进程装配点都调 `from_env()`，未配置时为 None（零行为变化）。
7. **测试**：内嵌固定 ES256 测试密钥对（PEM + JWK x/y 常量）；mock JWKS HTTP 服务（仓库既有 TcpListener 模式）+ jsonwebtoken 签发令牌。覆盖：有效令牌角色映射（admin/operator/viewer）、scopes 映射、kid 未命中触发刷新（先发空 JWKS 再发全量）、过期/错误 aud/错误 iss/坏签名/HS256 拒绝/无 sub 拒绝、静态凭据回退不受影响、未配置 OIDC 时 JWT 被拒。

## Risks / Trade-offs

- [异步化触碰 30+ 行授权调用] → 全部机械 `.await`；编译器兜底，测试覆盖 handler 授权路径（既有 API 测试）。
- [OIDC 提供方 JWKS 短暂不可用导致缓存过期后全部拒真] → TTL 过期后旧键继续用于**已知 kid**（stale-while-revalidate：kid 命中即通过并后台... 简化为：过期缓存仍可解码已知 kid，仅 kid 未命中才强制刷新）——v1 采用「缓存永续用于已知 kid + kid 未命中刷新一次」，避免可用性悬崖；密钥被吊销的窗口由 token exp 上限约束（文档建议 IdP 签发短时令牌）。
- [clock skew] → jsonwebtoken 默认 leeway 60s。
- [角色映射权限提升] → 只映射白名单三角色，claim 中其他值忽略；scopes 限既有文法。

## Migration Plan

未配置 OIDC 环境变量时行为逐字节不变；配置后静态凭据仍优先（先静态后 OIDC），回滚 = 移除环境变量。

## Open Questions

无。
