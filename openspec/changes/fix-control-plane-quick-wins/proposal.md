## Why

P2 控制面批（`openspec/CODE_REVIEW_2026-09-29.md`）剩余快速修复项：

1. **hub_problem 存储故障 → 400**：`StorageUnavailable` 与 `Storage(_)` 落入 `_ => BAD_REQUEST`（`lib.rs:3608`）——服务端存储故障被报成**客户端 400 错误**。而 `hub_stream` 单独把 StorageUnavailable 正确映射为 503。`GenerationConflict` 应为 409（语义冲突）而非 400。
2. **分页溢出 5 副本**：`(page - 1) * page_size` 在 5 处内联副本（`:915,2620,2692,3741,3806`）未随 helper 修复——`?page=usize::MAX` 在 debug 构建直接 panic（axum 不捕获 panic），release 下回绕。
3. **`${file:}` 任意读取面**：`resolve_file`（`secret.rs:315`）对路径零约束——`${file:/etc/shadow}` 会读入。控制面配置端点脱敏只按 key 名匹配（password/token/secret），文件内容放在中性 key 下原样返回——需认证但构成文件内容外泄通道。
4. **`pipeline::Pipeline` 死 API**：全仓库无生产调用（仅 pipeline/mod.rs 自身），且其语义与执行器矛盾（静默剥离 ack，`pipeline/mod.rs:69-85`）——留在 pub API 里会误导用户。

## What Changes

- `hub_problem`：`StorageUnavailable | Storage(_)` → 503 `storage_unavailable`；`GenerationConflict` → 409 `generation_conflict`
- 分页溢出：5 处内联 `(page - 1) * page_size` 改用 `page.saturating_sub(1).saturating_mul(page_size)` 饱和算术（对齐 `page_items` helper 的既有修复）
- `${file:}` 路径沙箱：`resolve_file` 限制为绝对路径且不允许 `..` 组件（防止配置文件读取面被武器化为任意文件通道）；错误信息不回显文件内容
- 删除 `pipeline::Pipeline`（死 API + 语义矛盾）

## Capabilities

### New Capabilities
（无）

### Modified Capabilities
- `control-plane-api`（hub_problem 错误映射语义）；`secret-references`（file: 路径约束）

## Impact

- `crates/arkflow-server/src/lib.rs`（hub_problem 映射 + 5 处分页修复）
- `crates/arkflow-core/src/secret.rs`（resolve_file 沙箱）
- `crates/arkflow-core/src/pipeline/`（删除）

## Non-goals

- 不重构全部错误映射为类型驱动（后续）
- 不实现 ${file:} 的路径白名单配置（当前只封 `..` 与要求绝对路径）
