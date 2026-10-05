# Proposal: refactor-server-file-splits

## Why

P3 结构债的 server 部分（2026-10-05 核实）：`arkflow-server/src/lib.rs` 11,271 行（源码 4.2k + 内联测试 7k，96 条路由、三个 router builder、双 API 巨石）；`agent.rs` 9,135 行（源码 3.4k + 测试 5.6k，配置/checkpoint 恢复/内核 spawn/注册表适配/资源采样/主循环/会话/命令分发八种关注点混排）。纯文件级拆分，零行为变化。

## What Changes

- **lib.rs 源码区 → `src/api/` 模块**：`api/mod.rs`（`router()`/`observability_router()`/`hub_router()` 三个 builder + 共享 AppState/extractor/中间件接线）、按路由域分 handler 模块（`configuration.rs`、`jobs.rs`、`nodes.rs`、`rollouts.rs`、`operations.rs`、`diagnostics.rs` 等，具体边界以编译器与内聚性裁决）；`HubTlsListener` 归位 `api/tls.rs` 或就近模块；内联 `#[cfg(test)] mod tests`（~7k 行）整体移至 `src/api/tests.rs`（或 `src/tests.rs`）。lib.rs 保留 `pub mod` 声明、`serve`/`serve_hub` 与 crate 级 re-export。
- **agent.rs → `src/agent/` 目录**：`config.rs`（NodeAgentConfig）、`checkpoint.rs`（SharedCheckpointStore/checkpoint_repository/恢复）、`kernel.rs`（内核 job spawn/detached teardown/registry 适配）、`resources.rs`（主机资源采样与指标）、`session.rs`（注册/重试/主循环/会话/心跳上报）、`commands.rs`（execute_command/execute_job_operation 分发——**字符串命令协议保持不动**）；`mod.rs` + `tests.rs`（~5.6k 行测试整体搬移）。`crate::agent::X` 外部引用路径保持不变。
- hub/ 既有 16 个子模块的 `use super::*` 本次不改（避免超范围扩散），仅新拆模块用显式/最小 use。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

（无——纯内部结构移动；`pub fn router/hub_router/serve/serve_hub` 与 `agent::run` 等公共入口签名不变）

## Impact

- `crates/arkflow-server/src/lib.rs` → `src/api/*` + 瘦身后的 lib.rs；`crates/arkflow-server/src/agent.rs` → `src/agent/*`
- 验证：`cargo test -p arkflow-server`（397 lib + 14 集成测试）+ clippy/fmt；bootstrap 等集成测试覆盖 router 接线。
