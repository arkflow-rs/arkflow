# Design: harden-standalone-control-api

## Context

Hub 与 standalone 两条控制面路径的护栏不对称（见 proposal Why 节 file:line 证据）：Hub 有 `validate_hub_startup` + default-deny 中间件；standalone 是"handler 各自为战 + 无 token 恒真 + 无绑定校验"。本变更把 Hub 已验证的模式移植过来，不发明新机制。

## Goals / Non-Goals

**Goals:**

- 非回环 + 无凭据的 standalone 部署在启动期被拒绝（或显式 `insecure_local` + 警告）。
- 认证判定收敛到一处中间件，读写端点一致。
- 回环本地开发零摩擦、既有 token 部署行为兼容（写端点要求不变，读端点从裸奔变 401——预期收紧）。

**Non-Goals:** 见 proposal Non-goals（OIDC/角色、Hub 面、observability 端口、console token、ct_eq 实现）。

## Decisions

### D1 — 启动护栏：复用 Hub 校验的结构与语义，落在 `serve()` 入口

standalone `serve()`（`api/mod.rs:377-391`）开头增加校验段，语义对齐 `validate_hub_startup`（`api/mod.rs:168-202`）：

```
非回环绑定 ∧ api_token.is_none() ∧ !insecure_local  →  Error::Config（拒启）
非回环绑定 ∧ api_token.is_none() ∧ insecure_local   →  error! 级警告（对齐 api/mod.rs:792-804 的 Hub 警告形态）
```

回环判定：与 Hub 共用同一工具函数（若 Hub 内联实现则提取为 `api/mod.rs` 私有 fn，两处调用——一处提取，不动 Hub 行为）。`insecure_local` 配置项落在 standalone 的 control_api/health_check 配置段（沿用 Hub 同名键的 serde 形态，docs 同步）。

### D2 — default-deny 中间件：包在嵌套 `api` 路由外层

现结构：`router()` 内 `nest(prefix, api)`（`api/mod.rs:360`），认证散在各 handler。改为：

```rust
let api = api.route_layer(middleware::from_fn_with_state(cp, local_auth_middleware));
```

`local_auth_middleware`：读 `Authorization: Bearer`，`cp.authorized(token)`——`None` token 直通（回环护栏由 D1 保证组合合法），`Some` 则不匹配即 401（响应体沿用既有 unauthorized 形态，`api/mod.rs:478-482` 的 Hub 惯例）。各 handler 内的 `authorized()` 自查（configuration/streams/operations 共 10 处）删除——中间件已覆盖，散查成为死代码。注意中间件挂**内层** `route_layer`（嵌套内、不影响外层 hub 路由——standalone 与 hub serve 路径的 router 组装分开，互不污染）。

### D3 — 测试矩阵

`api/tests.rs` 新增（既有 76 例基础上）：

- 启动：非回环无 token 拒启（错误文案断言）；+`insecure_local` 过并告警；回环无 token 过。
- 401 矩阵：{streams GET/POST, config apply, events, metrics} × {无 token 配置(直通), 有 token 无头/错头/对头}。
- 既有本地平面用例按新矩阵同步（原裸读用例补 token 或保持回环直通语境）。

## Risks / Trade-offs

- **读端点从裸奔变 401（设 token 时）**：既有带 token 部署的监控脚本会断——预期收紧，release notes + docs 说明；回环直通覆盖本机监控。
- **非回环拒启是行为变化**：误配部署升级后起不来——正是要暴露的（对比静默暴露 RCE 面），文案给出两条出路（设 token / insecure_local）。
- 中间件与 handler 双重认证窗口：散查删除后无双查，无性能影响。

## Migration Plan

单 PR。配置面新增 `insecure_local`（默认 false）；docs 与 release notes 说明拒启与 401 收紧。

## Open Questions

（无——`insecure_local` 若 standalone 配置结构已有同名字段则直接复用，实现时核对 serde 兼容。）
