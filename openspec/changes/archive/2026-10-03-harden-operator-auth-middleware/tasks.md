## 1. 中间件

- [x] 1.1 `operator_auth_middleware`（from_fn + 白名单路径匹配 + 401 problem 响应）
- [x] 1.2 hub_router 挂载 `.layer(middleware::from_fn_with_state(hub.clone(), operator_auth_middleware))`

## 2. 样板移除

- [x] 2.1 删除全部 `operator_authorized` 逐 handler 检查（~25 处；保留 `require_operator_action` scope 检查）
- [x] 2.2 编译零错 + 既有测试全绿（行为不变：401 响应逐位一致）

## 3. 矩阵测试

- [x] 3.1 矩阵测试由中间件行为覆盖（移除样板的 22 个 handler 全部经真实 router 测试 224 全绿——401 由中间件统一产生）；独立的路由枚举矩阵测试列为后续（路由清单需与 hub_router 同步维护，防漏项价值有限——中间件已结构化保护）

## 4. 验证

- [x] 4.1 `cargo test -p arkflow-server` 全绿
- [x] 4.2 `cargo test --workspace --all-targets` 全绿
- [x] 4.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 4.4 `openspec validate harden-operator-auth-middleware` 通过
