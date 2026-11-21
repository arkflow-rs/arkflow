# Tasks

## 1. workspace.lints

- 1.1 根 `Cargo.toml` 增加 `[workspace.lints.rust]`（`unsafe_op_in_unsafe_fn`）与 `[workspace.lints.clippy]`（`all`、`dbg_macro`、`todo`），附纪律来源注释
- 1.2 四个成员 crate（arkflow-core/arkflow-plugin/arkflow-server/arkflow）增加 `[lints] workspace = true`
- 1.3 `cargo clippy --locked --workspace --all-targets -- -D warnings` 全绿（新 lints 零违规）

## 2. rand 声明集中化

- 2.1 `arkflow-plugin/Cargo.toml`：`rand = "0.10"` → `rand = { workspace = true }`（唯一用法 qdrant `rand::random::<u64>()` 在 0.9 可用）
- 2.2 `Cargo.lock` 更新为 plugin rand 0.9.5，`cargo check --locked` 通过

## 3. 锁卫生测试扩展

- 3.1 `dependency_lock_hygiene.rs` 新增 `workspace_crates_declare_rand_only_through_the_workspace`：扫描成员清单，非 workspace 表的 rand 声明即失败
- 3.2 模块注释记录 rand 三代共存的传递根因（0.8/0.9/0.10 各自的钉死方），说明为何约定断言而非单版本断言
- 3.3 两个测试通过（含对 `crates/.DS_Store` 等非 crate 条目的健壮跳过）

## 4. CI --locked

- 4.1 `rust.yml`：clippy、build、nextest、`cargo test -p arkflow-plugin` 加 `--locked`
- 4.2 `coverage.yml`：`cargo llvm-cov` 加 `--locked`
- 4.3 `benchmark.yml`：`cargo run` 加 `--locked`

## 5. 规格

- 5.1 新 capability `ci-hygiene-gates` delta（ADDED：`--locked` 门禁、workspace.lints 纪律、rand 集中化约定），`openspec validate` 通过

## 6. 收尾

- 5.1 `cargo fmt --all -- --check` 全绿
- 5.2 更新审查路线图记忆（批次 3-6 销账 + 批次 7 闭环）
