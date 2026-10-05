# Tasks: refactor-p3-misc-batch

## 1. Rust 杂项

- [x] 1.1 `crates/arkflow-plugin/src/lib.rs`：失败不缓存可重试——顶层 + 每 kind 成功闩锁（实现期发现各 kind `init()` 非幂等，仅顶层闩锁会使断点续跑撞重复注册，故加每 kind 闩锁实现「跳过已成功、从首个失败续跑」）；测试：`initialize_latches_success`、`init_step_latches_success_and_retries_failure`（fn 指针注入先失败后成功）
- [x] 1.2 `crates/arkflow-server/src/agent.rs`：`SharedCheckpointStore::block_on` 改常驻 worker（OnceLock 单例线程 + flume 有界 64 通道 + std oneshot 回执 + 逐命令 catch_unwind）；错误字符串保持；测试：`checkpoint_store_serves_concurrent_operations_from_one_worker`、`checkpoint_worker_isolates_panicking_commands`
- [x] 1.3 `crates/arkflow-server/src/storage/postgres.rs`：`q()` 加 `OnceLock<Mutex<HashMap>>` 缓存（重写逻辑抽为 `rewrite_sql`）；测试：`placeholder_rewrites_are_cached_by_sql_text`
- [x] 1.4 `crates/arkflow-core/src/executor/kernel_handle.rs`：`Completion` 升级为 `CompletionSlot`（结果槽 + watch 版本通道），watcher 先订阅再检查、`changed().await` 替代 50ms 轮询；abort-on-drop 守卫取消（会改公共返回类型且泄漏面已被 catch_unwind 不变量覆盖，见 design.md 调整说明）；测试：`watchers_wake_from_the_version_channel_without_polling`
- [x] 1.5 `crates/arkflow-core/src/wal/mod.rs`：补全 `append` 的截断 doc comment（指向 `call_store` 的阻塞调用分流）

## 2. console

- [x] 2.1 `src/queries.ts`：`refetchInterval` 改用 `JOB_DETAIL_INTERVAL_MS` / `ROLLOUT_DETAIL_INTERVAL_MS`
- [x] 2.2 `src/queries.ts` 新增 `useJobVersions` + `useRollbackJobUpgrade`（成功后 invalidate job-versions）；`src/features/jobs.tsx` JobVersions 改走 react-query
- [x] 2.3 引入 ESLint（flat config：js/ts recommended + react-hooks + prettier 收尾 + `_` 前缀豁免），修复 9 处真实未用变量、5 处刻意的 effect-setState 模式行内豁免注明理由、job-dag/job-editor 的动态 JSON `any` 文件级豁免；`lint` script + `console.yml` 门禁；存量 0 error（7 条 exhaustive-deps advisory warning 保留）

## 3. 验证与收尾

- [x] 3.1 `cargo test -p arkflow-plugin -p arkflow-server -p arkflow-core` + clippy + fmt 全绿
- [x] 3.2 console：`npm run lint`（0 error）/ `typecheck` / `format:check` / `test`（61/61）/ `build` 全绿
- [x] 3.3 openspec validate + 归档，CODE_REVIEW_2026-09-29.md / PLANNING.md 划项
