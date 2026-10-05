# Tasks: refactor-executor-file-splits

- [x] 1. remote.rs → `executor/remote/` 目录（wire/codec/transport/auth/manager/mod/tests），外部路径不变
- [x] 2. window.rs → `executor/window/` 目录（aggregate/firing/operator/mod/tests），外部路径不变
- [x] 3. 兼容壳清理：删 `run_graph_with_gate`、`run_graph_with_hooks_and_gate`（调用点改 `run_graph_with_hooks`）、`run_job_with_metrics`（`run_job` 直连）
- [x] 4. 验证：`cargo test -p arkflow-core` 全绿 + `cargo clippy --workspace --all-targets` + `cargo fmt`
- [x] 5. openspec validate + 归档
