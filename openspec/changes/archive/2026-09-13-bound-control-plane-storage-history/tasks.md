## 1. 存储层清扫

- [x] 1.1 `storage.rs` 新增 `prune_processed_outbox(cutoff, max)`:`processed_at_ms IS NOT NULL` 域的 24h 窗口删 + 4096 计数删(照抄 `prune_operation_history` 的 DELETE 双界写法),接通 StorageActor 命令与 trait
- [x] 1.2 `storage.rs` 新增 `prune_terminal_attempts(cutoff, max)`:`state NOT IN ('queued','dispatched','acknowledged','running') AND finished_at_ms < ?1` + 4096 计数删,接通 StorageActor
- [x] 1.3 删除 vestigial `cp_job_observations` 的 `CREATE TABLE` 定义(grep 复核全仓库零读写引用)

## 2. Hub 包装与 cadence 拆分

- [x] 2.1 `hub.rs` 新增 `prune_outbox_history()` / `prune_attempt_history()` 包装(常量 24h/4096,镜像 `prune_operation_history`)
- [x] 2.2 `lib.rs` 新增 60s 维护任务,执行:`prune_outbox_history` + `prune_attempt_history` + 既有三个 prune + `prune_events(2048)`;从 reconcile tick 摘除这五项,保留 `expire_attempts`/`expire_stale_job_operations`/`schedule_periodic_checkpoints`/三类 reconcile

## 3. 测试

- [x] 3.1 outbox 清扫测试:插入已处理(超窗/超量)与未处理行,断言超窗已处理行被删、4096 界生效、未处理行(含已认领未处理)原样保留、`outbox_pending/claimed` 统计口径不变
- [x] 3.2 attempts 清扫测试:终态超窗行被删、4096 界生效;active 行(`cp_one_active_attempt` 域)清扫后原样保留
- [x] 3.3 cadence 测试:维护任务触发后各表收敛;reconcile tick 路径不再调用 retention prune(既有行为性 sweep 测试不回归)
- [x] 3.4 全量 `cargo test --workspace --all-targets` 与 `cargo clippy --workspace --all-targets` 通过(注意:fmt 会搅动未触碰文件,只提交本 change 相关改动)

## 4. 收尾

- [x] 4.1 `docs/docs/control-plane/3-operations.md` 保留段落补一句:outbox/attempts 历史同受有界保留(与 operations/audit 并列)
- [x] 4.2 `/opsx:verify` 校验后 `/opsx:archive` 归档
