# Proposal: close-hardening-loose-ends

## Why

The distributed hardening pass (#1239) left three loose ends that keep "authenticated by convention" from being "authenticated by construction":

1. **未认证数据面 API 仍编译在公开面上。** `NetworkManager` 保留了三个绕过会话认证的边构造入口：`open_edge`（`crates/arkflow-core/src/executor/remote.rs:1530`）、`open_edge_with_stream`（`remote.rs:1555`）、`open_edge_deferred`（`remote.rs:1601`），以及配套的 `legacy_job` 单 Job 登记路径（`remote.rs:1183-1186`）。它们目前只被内嵌测试消费（`remote.rs:3402`、`executor/tests.rs:4564-4567, 4674`），生产侧靠调用者自觉：Agent 在无凭据时根本不创建监听器（`crates/arkflow-server/src/agent.rs:1674-1695`）、图构建只走会话 API（`graph.rs:867/924`）——但 `arkflow-core` 本身不阻止未来的调用者用一个 `credentials: None` 的 manager 绑定生产 TCP 监听（`serve_stream_inner:1897` 的认证分支以凭据存在为前提）。
2. **`BarrierCoordinator::complete` 内有死代码模糊契约。** `barrier.rs:341-358` 构造了 `TaskAttemptSnapshot` 向量后立即丢弃（`let _ = attempts`），真正有效的只有注释里那句"manifest 持久化归调用方"。死代码让读者误以为这里有未完成的持久化逻辑。
3. **boot 变更失效逻辑在 Hub 内复制粘贴。** Agent 进程身份（`boot_id`）变化后的 job_start 失效处理，在 `register_after_auth`（`hub.rs:2491-2527`）与 `report`（`hub.rs:2687-2723`）各有一份逐行同构的拷贝。这是安全相关的失效语义，两份拷贝是漂移出 bug 的典型温床。

Why now：8 个变更全部完结、无并行开发在飞，是零行为冲突窗口；且任何后续工作（placement、控制面演进）都要穿过这些文件，现在收口让后面的每个 diff 更小。

## What Changes

- 将 `NetworkManager` 的未认证边构造入口（`open_edge` / `open_edge_with_stream` / `open_edge_deferred` 及其依赖的 `legacy_job` 登记路径）收进 `#[cfg(test)]`，并让 `bind_tcp` 在未配置凭据时显式报错拒绝绑定；内部测试调用点迁移到既有会话 API（`open_edge_deferred_for_job` / `open_edge_with_stream_for_session` 等，`remote.rs` 内嵌测试已有示范）。
- 删除 `BarrierCoordinator::complete` 中的死代码，把"协调器职责止于全部参与者上报，manifest 持久化归 Agent/Engine 接线"从行内注释升级为文档化契约。
- 将 register/report 两处的 boot 变更失效逻辑抽取为单一 `Hub` 方法，两个入口共用。

所有生产路径零行为变化；无配置项、schema、组件注册变更。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `authenticated-network-shuffle`: 新增一条需求——未认证传输面 SHALL 仅在测试边界内可达（无凭据的 manager 不得在非测试构建中开边或绑定 TCP 监听）。该能力由待归档的 `harden-remote-data-plane` 变更创建（主规格尚未落盘）；本变更的 delta 采用 ADDED 形式，两种归档顺序下均可正确合并。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`：API 可见性收口 + `bind_tcp` 凭据校验 + 内嵌测试迁移。
- `crates/arkflow-core/src/executor/tests.rs`：远程集成测试迁移到会话 API。
- `crates/arkflow-core/src/executor/barrier.rs`：死代码删除与契约文档。
- `crates/arkflow-server/src/hub.rs`：boot 失效逻辑去重（纯内部重构）。
- 不影响 README 组件清单、example manifest、docs 页面（无注册/配置变更）。
