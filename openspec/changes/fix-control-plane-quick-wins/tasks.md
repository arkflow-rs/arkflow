## 1. hub_problem 映射修正

- [x] 1.1 `StorageUnavailable \| Storage(_)` → 503 `storage_unavailable`；`GenerationConflict` → 409 `generation_conflict`；单测断言两个映射

## 2. 分页溢出修复

- [x] 2.1 5 处 `(page - 1) * page_size` 改为 `page.saturating_sub(1).saturating_mul(page_size)`

## 3. ${file:} 路径沙箱

- [x] 3.1 `resolve_file` 拒绝含 `..` 的路径与相对路径；单测（合法绝对路径通过 / `..` 拒绝 / 相对路径拒绝）

## 4. Pipeline 死 API 删除

- [x] 4.1 删除 `crates/arkflow-core/src/pipeline/` 模块（含 pub 导出）；编译零错（无生产调用方）

## 5. 验证

- [x] 5.1 `cargo test --workspace --all-targets` 全绿
- [x] 5.2 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 5.3 `openspec validate fix-control-plane-quick-wins` 通过
