## Why

P2 控制面批（`openspec/CODE_REVIEW_2026-09-29.md`「operator 认证样板无中间件化 + 无 401 矩阵测试」）。当前 Hub 的 operator 认证是**逐 handler 手写样板**（~40 处 `if !hub.operator_authorized(bearer(&headers)) { return problem(...) }` / `require_operator_action(...)`）——新增 handler 忘加检查就是认证绕过，且无"逐路由断言 401"的矩阵测试兜底。审查核实 94 个 handler 全覆盖无遗漏，但这是**纪律维持**而非**结构保证**。

## What Changes

- **axum middleware 层**：`operator_auth_middleware` 在路由层统一检查 operator token（`hub.operator_authorized`），白名单豁免（健康探针、metrics、组件目录、OIDC 端点、agent 路由——agent 用 node token 在 hub 方法内部认证）。
- **移除逐 handler 的 `operator_authorized` 样板**（中间件已覆盖）；**保留** `require_operator_action` 的 scope 检查（中间件只保证"是有效 operator"，scope 是 handler 级语义）。
- **401 矩阵测试**：遍历 hub_router 的全部路由（不含白名单），不带 token 发请求，断言全部返回 401。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `control-plane-identity`：operator 认证 SHALL 在路由层统一执行（中间件），新增 operator 路由自动受保护；矩阵测试 SHALL 钉住全部路由的 401 行为。

## Impact

- `crates/arkflow-server/src/lib.rs`（middleware + 移除 ~25 处样板 + 矩阵测试）
- 无 wire/API 行为变更（401 响应与现状逐位一致——同样的错误码与 body）。

## Non-goals

- 不改 RBAC scope 检查（`require_operator_action` 保留在 handler 内）。
- 不改 agent 路由认证（node token 在 hub 方法内部）。
- 不改 OIDC 流（公开端点白名单）。
