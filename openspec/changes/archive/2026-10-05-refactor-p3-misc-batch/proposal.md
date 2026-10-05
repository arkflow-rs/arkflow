# Proposal: refactor-p3-misc-batch

## Why

`openspec/CODE_REVIEW_2026-09-29.md` P3 结构债中的杂项经 2026-10-05 核实仍全部存在：plugin `initialize()` 把首次失败永久缓存进 `OnceLock`（瞬时失败后进程内永不可恢复）；Agent 的 `SharedCheckpointStore` 每次操作新建一个 OS 线程加一个 tokio runtime；PostgreSQL 后端的 `q()` 占位符重写每次执行重新分配重算；kernel watcher 以 50ms 轮询空转且句柄 detach（泄漏兜底仅靠注释不变量）；`wal/mod.rs` 留有被截断一半的文档句；console 死常量与硬编码字面量自相矛盾、JobVersions 绕过 react-query 数据层、无任何 linter。这些是 v1.0 前风险最低、性价比最高的一批收尾。

## What Changes

- **plugin init 失败可重试**：`INITIALIZATION` 只缓存成功（`Mutex<Option<()>>`，成功才落位）；失败返回错误且下次调用重新执行注册链。注册冲突等真实错误仍每次报错——变化仅在"瞬时失败不再被永久缓存"。
- **SharedCheckpointStore 常驻 worker 化**：`block_on` 的"每操作一线程一 runtime"改为进程内单个常驻 worker 线程 + 有界 mpsc 命令通道 + oneshot 回执；`CheckpointStore` 契约（put/get/delete、错误语义）逐位不变。
- **`q()` 重写缓存**：`?N→$N` / `INSERT OR IGNORE` 方言重写结果按 SQL 文本缓存（`OnceLock<Mutex<HashMap>>>`）；调用点全部为常量字面量，缓存规模有界。
- **kernel watcher 唤醒化**：completion 槽解析时经 `Notify` 唤醒等待者，替代 50ms 轮询；`watcher()` 返回的 JoinHandle 包 abort-on-drop 守卫，消除对"调用方必须 await"的隐性依赖（catch_unwind 兜底不变量不动）。
- **wal 文档修复**：补全 `wal/mod.rs` 中被截断的 doc comment 半句。
- **console 三项**：`JOB_DETAIL_INTERVAL_MS` / `ROLLOUT_DETAIL_INTERVAL_MS` 接入 `queries.ts` 的 `refetchInterval`（删除重复字面量）；JobVersions 改走 react-query（新增 `useJobVersions` + rollback mutation，重构 `features/jobs.tsx` 内联 useEffect）；引入 ESLint（typescript-eslint + react-hooks + prettier 兼容，flat config，`lint` script，`console.yml` CI 门禁，存量清零）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `component-registry-export`: plugin `initialize()` 失败语义从"首次失败永久缓存"改为"失败不缓存、后续调用重试注册"（成功仍只执行一次）。

## Impact

- `crates/arkflow-plugin/src/lib.rs`（init 重试 + 单测）
- `crates/arkflow-server/src/agent.rs`（SharedCheckpointStore worker 化 + 并发/关停测试）
- `crates/arkflow-server/src/storage/postgres.rs`（q() 缓存）
- `crates/arkflow-core/src/executor/kernel_handle.rs`（watcher Notify + abort-on-drop）
- `crates/arkflow-core/src/wal/mod.rs`（文档句）
- `console/`：`src/api.ts`、`src/queries.ts`、`src/features/jobs.tsx`、`eslint.config.js`、`package.json`（+devDependencies）、`.github/workflows/console.yml`
- 无配置面 / 协议 / 公共 API 变化。
