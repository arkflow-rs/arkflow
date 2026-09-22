## Context

`hub-oidc-auth` 已提供 JWT bearer 验证（`OidcAuthenticator`：JWKS 验证 + claims→principal 映射）与 Hub 挂载（`Hub::with_oidc`）。授权码流需要：IdP discovery（拿 authorization/token endpoint）、code 交换（form POST）、服务端会话（opaque cookie ↔ principal）、以及让既有授权路径感知会话 cookie。`bearer()`（`lib.rs:2801`）目前只读 Authorization 头；全部 handler 经它取凭据后调 `operator_*`。

约束：不新增依赖；axum 0.8 handler 均为 async；`rand`（含 OsRng）已在 server deps；会话不持久化（v1 内存表）。

## Goals / Non-Goals

**Goals:**
- 浏览器经 IdP 登录 Console：`/auth/oidc/login` → IdP → `/auth/oidc/callback` → 会话 cookie → Console 可用。
- 会话参与既有授权解析（RBAC/审计语义零变化）；静态凭据与 OIDC bearer 全部保留。

**Non-Goals:** PKCE、持久化会话、refresh token、Console 前端改动、多 IdP（见 proposal）。

## Decisions

1. **装配收拢为 `OidcFederation::from_env()`（async）**：issuer/audience 必填；client_id/secret/redirect_uri 三者齐备 → 启用登录流并做一次 discovery（拿 authorization/token endpoint，缓存）；失败 → 告警 + 降级为仅 bearer。备选「discovery 惰性到首次 login」——拒绝：登录时才失败更难排查，启动告警直观。
2. **端点**：`GET /auth/oidc/login`（随机 state 32B hex → `arkflow_oidc_state` cookie（HttpOnly/SameSite=Lax/Max-Age 600s）+ 302 授权端点）、`GET /auth/oidc/callback`（state 校验 → code 换 id_token → OidcAuthenticator 验证 → 8h 会话 → `arkflow_session` cookie → 303 `/`）、`GET /auth/oidc/logout`（删会话 + 清 cookie）。仅登录流启用时注册（未启用时访问 404，与现状一致）。
3. **会话桥接进授权路径**：`bearer()` 无 Authorization 头时回退读 `arkflow_session` cookie 并返回 `session:<sid>`（返回类型改为 `Option<String>`，调用点机械更新）；`operator_principal` 开头解析 `session:` 前缀 → 会话表查找（TTL 内）→ principal。RBAC/审计语义零变化——principal 就是既有类型。
4. **会话表**：`Arc<Mutex<HashMap<String, (OperatorPrincipal, Instant)>>>`，8h TTL 惰性清理（访问时剔除过期项）；随机会话 id 来自 OsRng 32B hex（hub.rs 已有 OsRng 先例）。重启清空 → 浏览器重新登录，可接受。
5. **state 防伪造**：登录时写 state cookie，callback 时要求 query `state` 与 cookie 严格相等（ct 比较），否则 401；用后即清。
6. **code 交换**：form POST token_endpoint（grant_type/client_id/client_secret/redirect_uri/code），取 `id_token` 交给 OidcAuthenticator（JWKS 验证 + claims 映射，与 bearer 模式同一套语义）。
7. **失败语义**：state 不匹配/code 交换失败/id_token 验证失败 → 401（页面正文给稳定错误串，不含 IdP 响应细节以外信息）。

## Risks / Trade-offs

- [会话在内存、重启清空] → 用户重新登录即可；无状态服务端（无 DB 迁移）。
- [无 PKCE] → server-side client_secret + HttpOnly state cookie 已覆盖主流威胁；IdP 强制 PKCE 时后补。
- [8h 会话窗口内无吊销] → logout 清服务端会话；文档建议短 TTL 的 IdP 令牌（会话不依赖 id_token 过期，但 id_token 验证本身带 exp）。
- [discovery 启动依赖 IdP 可达] → 失败仅降级 bearer 模式并告警，不阻塞启动。

## Migration Plan

未配置 client 凭据时零行为变化（路由不注册、会话逻辑不激活）；回滚 = 移除三个新环境变量。

## Open Questions

无。
