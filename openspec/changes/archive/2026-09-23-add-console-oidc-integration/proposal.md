## Why

`hub-oidc-auth`/`add-hub-oidc-login` 已交付 JWT bearer 验证与浏览器授权码登录（login/callback/logout + 会话 cookie），但 Console（Vite SPA）尚不感知这套机制：用户遇到 401 时没有引导路径，也无法在页面内登出——OIDC 故事的「最后一块」是把 Console 接上。服务端只需一个免认证的 status 端点；Console 端改动集中在 `api.ts` 的统一请求出口与头部一个按钮。

## What Changes

- 服务端：新增免认证路由 `GET /api/v1/auth/oidc/status`，返回 `{login_enabled, authenticated, principal: {id, roles} | null}`——`login_enabled` 为登录流是否启用，`authenticated`/`principal` 由会话 cookie 解析。
- Console（`console/src/api.ts` + `app.tsx`）：
  - 启动时探测 status（缓存）；
  - `request()` 遇到 401 且登录流启用且未配置 VITE_API_TOKEN 时，重定向到 `/auth/oidc/login`（10s 内只重定向一次，防循环）；
  - 头部在已认证时显示 `Sign out` 按钮（调用 logout 端点后刷新页面）。
- 静态凭据（VITE_API_TOKEN bearer）路径完全保留，两种模式互斥不冲突。

## Capabilities

### New Capabilities

<!-- 无新能力：扩展 hub-oidc-auth。 -->

### Modified Capabilities

- `hub-oidc-auth`: 新增 OIDC status 端点需求（登录可用性/会话状态的探测面，供 Console 与运维脚本使用）。

## Impact

- `crates/arkflow-server/src/lib.rs`：status 路由 + handler + Hub 会话查询方法。
- `console/src/api.ts`、`app.tsx` + vitest 测试；`npm test`/`npm run build` 验证。
- 文档：部署页 OIDC 节补充 Console 集成说明（en/zh）。

## Non-goals

- 不做 Console 的完整登录页 UI（登录由 IdP 托管，Hub 只做重定向）；不做 token 管理界面。
- 不改既有 VITE_API_TOKEN 模式与后端授权语义。
