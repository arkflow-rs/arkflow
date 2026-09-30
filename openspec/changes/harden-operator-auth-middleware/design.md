## Context

Hub 有 ~94 个 handler，其中 ~40 个手写 operator auth 样板。两类检查并存：`operator_authorized`（仅验证"是有效 operator"）和 `require_operator_action`（验证 scope）。前者可统一到中间件，后者是 handler 级语义。

## Goals / Non-Goals

**Goals:** operator 认证路由层统一执行；新增路由自动受保护；401 矩阵测试钉住全部路由。

**Non-Goals:** scope 检查中间件化、agent 路由认证、OIDC 流（见 proposal）。

## Decisions

**D1 — `middleware::from_fn` + 白名单路径匹配。**
```rust
async fn operator_auth_middleware(
    State(hub): State<hub::Hub>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path();
    if PUBLIC_PATHS.iter().any(|p| path.starts_with(p)) {
        return next.run(req).await;
    }
    let headers = req.headers();
    if hub.operator_authorized(bearer(headers).as_deref()).await {
        next.run(req).await
    } else {
        hub_problem(hub::HubError::Unauthorized)
    }
}
```
白名单：`/ready`, `/live`, `/metrics`, `/system`, `/components`, `/schema`, `/auth/oidc/`, `/agent/`（agent 用 node token 在内部认证）。**注意** `/system` 需要检查——它可能包含敏感信息，如果有 auth 检查就不该在白名单里。

**D2 — 样板移除策略。**
`operator_authorized` 检查（~25 处）删除——中间件已覆盖。`require_operator_action` 检查（~12 处）保留——scope 是 handler 语义。区分方式：grep `operator_authorized` 删、grep `require_operator_action` 保留。

**D3 — 矩阵测试。**
构建 hub_router（真实 Hub），遍历其路由表（axum Router 有 `route_iter()` 或通过已知路径列表），对每个 operator 路由发不带 token 的 GET/POST/PUT/DELETE，断言 401。路径列表手工维护（新增路由忘加测试项 = 矩阵覆盖不全——比现状好得多）。
