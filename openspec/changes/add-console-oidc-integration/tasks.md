# Tasks: add-console-oidc-integration

## 1. 服务端 status 端点

- [ ] 1.1 `lib.rs`：注册免认证 `GET /auth/oidc/status` + handler（login_enabled/authenticated/principal）+ Hub 会话查询方法
- [ ] 1.2 测试：登录流启用+有效会话/无会话/登录流未启用三态

## 2. Console 集成

- [ ] 2.1 `console/src/api.ts`：`oidcStatus()` 探测（缓存）、`oidcLoginRedirect()`、`oidcLogout()`；`request()` 401 且登录可用且无静态 token 时重定向（10s 防循环）
- [ ] 2.2 `console/src/app.tsx`：头部已认证时显示 Sign out 按钮
- [ ] 2.3 vitest 测试：status 解析与 401 重定向防循环
- [ ] 2.4 `npm test` + `npm run build` 通过

## 3. 文档与全量验证

- [ ] 3.1 部署文档 OIDC 节补充 Console 集成说明（en/zh）
- [ ] 3.2 `cargo test --workspace --all-targets` 连续 2 轮全绿；clippy 无新告警；`pnpm docs:check` 通过
- [ ] 3.3 对照场景核对；`openspec validate add-console-oidc-integration` 通过
- [ ] 3.4 同步主 spec `hub-oidc-auth`（合并 delta）、归档 change、更新 PLANNING.md、提交
