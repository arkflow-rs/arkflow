## Why

P2 core 批（`openspec/CODE_REVIEW_2026-09-29.md`「RuntimeManager 两个并发缺陷」，控制面深潜核实）。两个缺陷都在 `crates/arkflow-core/src/runtime.rs`：

1. **注册索引复用 → job id / 状态命名空间冲突**（`runtime.rs:423-431`）：`register` 用 `entries.len()` 作为流的索引。`replace_config`（`:823-876`）的 stop→remove→register 序列下，remove 把 len 缩短后，存活流的重注册可能拿到与另一条存活流相同的索引。后果链：`start` 用该索引编译 `compile_stream(&config, index)` → JobSpec id = `stream-{index}`（`stream_compiler.rs:25-29`）→ ① `JobMetricsRegistry.register` 互相覆盖；② job id 参与 `state_namespace_prefix`（`job_runner_adapter.rs:158`）和路径拼接（`:410`）——两条活跃流可能**共享状态命名空间/目录**，状态互踩。

2. **stop/restart 竞态 → 已 stop 的流复活**（`runtime.rs:904-946` 与 `:955-1005`）：`restart` 在第一个锁块设置 `Restarting` 并取消 token 后释放锁，**锁外**取 handle；并发的 `stop` 看到 `Restarting` 落入"已 stop"分支（`Created | Stopped => return Ok(())`——`Restarting` 走 `_ => {}` 到 Stopping 转换），取走 handle、join 完成、置 Stopped 并返回 Ok。restart 侧随后 handle 为 None → wait_result Ok → 置 Stopped → **直接 `self.start(id)`**——stop() 已返回成功而流最终 Running。生命周期命令之间没有 per-entry 串行化（`active_operation_id` 只是记录，不构成互斥）。

## What Changes

- **索引改为全局单调**：`register` 不再用 `entries.len()`，改用 `next_index: AtomicU64` fetch_add——remove 后重注册永远拿到新索引，job id / 状态命名空间 / 路径在全注册史内唯一。既有显式 id 的流不受影响（id 覆盖 index-derived 名）；仅 legacy 无 id 流获得稳定唯一名。
- **stop/restart 竞态修复**：restart 的"取 handle"步骤改为在第一个锁块内完成（与状态转换原子化），或在 stop 的 `Stopping` 转换分支中也处理 `Restarting` 后的 start 复活。选择**最小侵入**：restart 的第一个锁块内同时完成"状态置 Restarting + token 取消 + handle 取出"（一个原子块），消除 stop 在两个锁块之间插入的窗口。
- 测试：① 替换配置后索引不复用（两流不共享 job id / 状态命名空间前缀）；② stop/restart 并发下 stop 返回 Ok 时流不 Running（用 cancelled token 的 cancel 观察复活）。

## Capabilities

### New Capabilities

（无——stream-runtime-control 为主 spec，读后确定 MODIFIED。）

### Modified Capabilities

- `stream-runtime-control`（待读 spec 后确认）：注册索引唯一性与 stop/restart 生命周期串行化条款。

## Impact

- `crates/arkflow-core/src/runtime.rs`（register 索引、restart 锁结构）
- 无配置面/存储格式变更；既有显式 id 流零行为变化。

## Non-goals

- 不重写 RuntimeManager 的锁架构（per-entry 串行化队列 / actor 化）。
- 不改 `replace_config` 的 stop→remove→register 语义本身（只修索引复用）。
- 不处理 `await_task` 超时 abort 后 WAL 锁残留的既有问题（`:1042-1059` 注释自认）。
