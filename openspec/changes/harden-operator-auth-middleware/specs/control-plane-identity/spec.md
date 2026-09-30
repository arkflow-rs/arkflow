## ADDED Requirements

### Requirement: Operator 认证 SHALL 在路由层统一执行

Hub 的 operator 路由 SHALL 在路由层通过中间件统一执行 token 认证——新增 operator 路由自动受保护，无需逐 handler 手写检查。白名单路径（健康探针、metrics、组件目录、OIDC 端点、agent 路由）SHALL 豁免中间件检查（agent 路由的 node token 认证在 hub 方法内部完成）。Handler 级的 RBAC scope 检查（`require_operator_action`）SHALL 保留在 handler 内（中间件只保证"是有效 operator"，scope 是 handler 语义）。

#### Scenario: 无 token 的 operator 路由全部 401

- **WHEN** 对任意 operator 路由发不带 Authorization 头的请求（矩阵测试遍历全部路由 × 方法）
- **THEN** 全部返回 401（与手写检查时的响应逐位一致）

#### Scenario: 新增路由自动受保护

- **WHEN** 向 hub_router 添加一个新的 operator 路由（无需手写 auth 检查）
- **THEN** 中间件自动对其执行 token 认证——忘加检查不再产生认证绕过

#### Scenario: 白名单路由不受影响

- **WHEN** 访问健康探针、metrics、组件目录或 OIDC 端点（不带 token）
- **THEN** 不返回 401（这些路由的设计就是公开的）
