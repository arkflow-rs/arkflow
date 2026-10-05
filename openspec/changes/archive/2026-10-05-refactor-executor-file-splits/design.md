# Design: refactor-executor-file-splits

## Context

P3 executor 结构债的机械拆分。remote.rs 与 window.rs 均为"实现 + 巨型内联测试模块"形态，五合一/三合一关注点混在单文件。

## Goals / Non-Goals

**Goals**：按关注点把两个巨型文件拆为目录模块；删除三个零价值兼容壳；外部引用路径（`executor::remote::*`、`executor::window::*`）与全部测试保持不变。

**Non-Goals**：不改任何逻辑/可见性语义（允许把原 `fn` 提为 `pub(super)`/`pub(crate)` 以跨子模块引用，但不得扩大到 crate 外）；不做 CheckpointHook 拆解；不做 state_journal 指针身份机制改动；不移动 graph.rs/envelope.rs。

## Decisions

- **mod.rs 全量重导出**：子模块项经 `pub(crate) use` / `pub use` 汇聚到 `remote::mod` 与 `window::mod`，外部 `use crate::executor::remote::X` 零改动（mod.rs 原本就 re-export 部分类型）。
- **测试整体搬移**：两个文件的 `#[cfg(test)] mod tests` 原样移到各自 `tests.rs`（`use super::*` 语义在目录模块下指向 `mod.rs`，重导出后覆盖原可见集；个别引用按编译器指引改 `use super::wire::*` 等）。
- **分段边界以编译器为裁判**：分段先按提案分配，编译错误驱动微调归属（如某 helper 仅被 manager 用则归 manager）。
- **兼容壳**：三个函数直接删除；`run_graph_with_gate`/`run_graph_with_hooks_and_gate` 的测试调用点改为 `run_graph_with_hooks`（两者本就是 no-op）；`run_job` 内联 `run_job_with_metrics_started` 调用。

## Risks / Trade-offs

- 纯移动 + 可见性微调，风险由 `cargo test -p arkflow-core`（约 1,111 项，含 remote 36 / window 95）兜底。
- 目录模块使某些 `pub(crate)` 项跨文件可见性变化——保持最小扩大原则。

## Migration Plan

无（内部结构，无外部可观测面）。
