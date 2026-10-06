## 1. 启动护栏

- [x] 1.1 回环判定提取为共用私有 fn（Hub `validate_hub_startup` 与 standalone 校验共用；Hub 行为零变化）
- [x] 1.2 standalone `serve()` 入口校验：非回环 ∧ 无 token ∧ 非 `insecure_local` → `Error::Config` 拒启（文案含两条出路）；`insecure_local` 时 error 级警告；配置项落 standalone 配置段（复用/新增 `insecure_local`，serde 形态对齐 Hub）
- [x] 1.3 测试：三类启动组合（拒启/告警放行/回环直通）

## 2. default-deny 中间件

- [x] 2.1 `local_auth_middleware`：Bearer 解析 → `cp.authorized()`；无 token 配置直通、有 token 不匹配 401（响应体沿 Hub 惯例）；`route_layer` 挂嵌套 `api` 内层
- [x] 2.2 删除各 handler 散落的 `authorized()` 自查（`api/configuration.rs` 8 处、`api/streams.rs:419`、`api/operations.rs:130`；`api/mod.rs:1120-1125` 的 helper 一并清理）
- [x] 2.3 测试：401 矩阵（读写端点 × 无 token 直通 / 无头 / 错头 / 对头）；既有本地平面用例同步

## 3. 文档与门禁

- [x] 3.1 standalone 运维页（en/zh）：启动护栏语义、`insecure_local`、读端点 401 收紧与监控脚本适配；observability 端口"勿暴露"的既有提示复核
- [x] 3.2 门禁：`cargo test -p arkflow-server` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`；`pnpm docs:check`
- [x] 3.3 契约复核：delta spec 每个 Scenario 有测试证据；tasks 勾选
