# Proposal: harden-standalone-control-api

## Why

Hub 控制平面有完整的启动护栏与 default-denied 路由中间件，**独立引擎控制 API（standalone `arkflow` 二进制的本地平面）没有抄这份作业**，三个缺口叠加成一条无认证接管路径：

1. **无 token 时认证恒真**：`crates/arkflow-core/src/control_plane.rs:119-124` 的 `authorized()` 在 `api_token` 为 `None`（默认，`crates/arkflow-core/src/config.rs:446`）时直接返回 `true`。
2. **无启动绑定校验**：`serve()`（`crates/arkflow-server/src/api/mod.rs:377-391` 起）不做任何"非回环绑定必须有凭据"检查——对比 Hub 的 `validate_hub_startup()`（`api/mod.rs:168-202`）拒绝非回环 `insecure_local`、缺 token/storage/凭据即拒启。
3. **读端点在认证之外**：本地平面 `router()`（`api/mod.rs:316-375`）只有变更类 handler 各自调用 `authorized()`（`api/configuration.rs:307/328/346/361/400/421/443/465`、`streams.rs:419`、`operations.rs:130`），读端点（streams/stream GET、events、system/status/nodes/components/schema/metrics，`diagnostics.rs:404-461`）**即使设了 token 也无需认证**。

组合后果：operator 把 `health_check.address` 设为 `0.0.0.0:8080`（默认 `127.0.0.1`）且未设 `control_api.api_token` 时，任意网络调用者可 POST `/api/v1/config/apply` 整体替换引擎配置——引擎配置接受 `python` 处理器（任意代码执行）与 `${file:}` 任意文件读取并经管道外传。流本身运行正常，误配完全不可见，直到被利用。

## What Changes

- **启动护栏**（对齐 Hub 语义）：standalone 模式 `serve()` 启动前校验——绑定地址非回环且 `api_token` 未设时拒绝启动，除非显式声明 `insecure_local`（复用 Hub 既有配置语义与文案风格）；回环绑定维持现状（本地开发零摩擦）。
- **default-deny 中间件**：本地平面嵌套 `api` 路由整体包一层认证中间件（对齐 Hub `operator_auth_middleware` 的结构，`api/mod.rs:460-482`），移除各 handler 内散落的 `authorized()` 自查；无 token（回环 + 未设）时中间件直通，其余全部要求 Bearer token。
- 校验失败与 401 响应形态对齐 Hub（`StatusCode::UNAUTHORIZED` + 既有错误体惯例）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `secure-durable-control-plane`: ADDED requirement——独立引擎控制平面 SHALL 应用与 Hub 同级的启动护栏（非回环绑定必须有凭据或显式 insecure_local）与 default-deny 路由认证（读写端点一致）。

## Non-goals

- 不给独立引擎引入 OIDC/角色体系（单 token 模型保持）。
- 不改 Hub 平面任何行为（`validate_hub_startup` 与 operator 中间件原样）。
- 不处理观测端口 `serve_observability`（`api/mod.rs:409-437`，默认回环、设计上无认证——文档任务在既有页面说明，不属本变更）。
- 不动 console 的 `VITE_API_TOKEN` 构建模式（前端侧另行处理）。
- 不改 `ControlPlane::authorized()` 的常量时间比较实现（正确，保持）。

## Impact

- `crates/arkflow-server/src/api/mod.rs`（serve() 校验段、本地路由中间件化、handler 内自查移除）。
- `crates/arkflow-core/src/config.rs`（standalone `insecure_local` 配置项，若 Hub 语义可复用则共用）。
- 测试：`api/tests.rs` 新增 401 矩阵（读+写 × 有/无 token × 回环/非回环）、启动拒绝用例；既有本地平面测试同步。
- 文档：standalone 运维页（en/zh）新增启动护栏与认证说明；`configuration-management` 相关页复核。
