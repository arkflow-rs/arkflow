# Design: refactor-server-file-splits

## Context

P3 server 结构债的机械拆分。lib.rs 与 agent.rs 均为"源码 + 巨型内联测试"单文件巨石。

## Goals / Non-Goals

**Goals**：handler 按路由域归位 `api/`；agent 按关注点归位 `agent/`；公共入口（`router`/`hub_router`/`observability_router`/`serve`/`serve_hub`/`agent::run` 等）签名与路径不变；全部测试原样通过。

**Non-Goals**：不改字符串命令协议（Non-goal 单独立项）；不改 hub/ 子模块的 `use super::*` 伪拆分；不做认证中间件化重构（已完成）；不移动 storage/oidc/metrics 等已独立模块。

## Decisions

- **api/mod.rs 承载 router builder 与共享状态**：三个 builder 与 AppState 类型放 `api/mod.rs`，handler 模块只装 `pub(crate) async fn` handler + 路由挂载辅助；模块边界先按提案、以"handler 间共享 helper 归属"编译裁决。
- **agent/ 拆分按既有注释分区**：agent.rs 源码区已有分节注释（config/checkpoint/kernel/resources/session/commands），按其切；跨节共享的小 helper 放 `mod.rs` 或归属最密集的子模块并 `pub(super)`。
- **测试整体搬移 + `use` 对齐**：两个测试模块分别移至 `api/tests.rs` 与 `agent/tests.rs`，`use super::*` 指向各自 mod.rs 的 re-export 面，缺项按编译器补。
- **可见性最小扩大**：仅允许 private → `pub(crate)`/`pub(super)`，不新增 crate 外可见项。

## Risks / Trade-offs

- 纯移动，`cargo test -p arkflow-server`（397 lib + 集成）兜底；api 接线由 bootstrap/集成测试覆盖。
- 拆分后文件数量增多——以每文件单一职责换可维护性。

## Migration Plan

无（内部结构）。
