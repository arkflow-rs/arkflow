## Why

长稳与规模上限验证(阶段 2 最后一项)立不了项的阻塞项:代码侦察(2026-09-13)发现两处**无界增长表**与一处**清扫成本随表爬升**的结构性问题——(1)`cp_outbox` 已处理行永不删除(`storage.rs` 无任何 `DELETE FROM cp_outbox`,`processed_at_ms` 只置位,每次 reconcile intent/register 都插新行);(2)`cp_attempts` 只被 `expire_attempts` 标记过期、从不删除,每次派发尝试累积一行;(3)`prune_operation_history`/`prune_stale_checkpoint_records`/`prune_audit_history` 三件套挂在 1s 的 reconcile tick 上(`lib.rs:425-427`),其中审计清扫每秒对 `cp_audit_events` 跑 `event_id NOT IN (SELECT … ORDER BY … LIMIT 100000)` 计数查询,成本随表大小线性增长,与单写者 StorageActor 争锁。带着已知无界表跑 soak,「零无界增长」断言只能豁免它们;带着每秒清扫跑规模阶梯,knee 测量被清扫开销污染。

## What Changes

- **`cp_outbox` 已处理行保留清扫**:新增 `prune_processed_outbox`,按 24h 窗口 + 4096 行双界(对称于 `prune_operation_history`)删除 `processed_at_ms` 非空的过期行;未处理行不受影响。
- **`cp_attempts` 终态行保留清扫**:新增 `prune_terminal_attempts`,按 24h 窗口 + 4096 行双界删除终态且 `finished_at_ms` 超窗的行(谓词与 `cp_operations` 清扫对称;active 状态行受 `cp_one_active_attempt` 唯一索引保护,绝不触碰)。
- **清扫与 reconcile tick 解耦**:三件套既有 prune 加上两个新 prune(以及 `prune_events(2048)`)移出 1s reconcile tick,改由独立的低频维护任务(60s 固定 cadence)执行;reconcile tick 保留行为性 sweep(`expire_attempts`、`expire_stale_job_operations`、`schedule_periodic_checkpoints`、三类 reconcile)。
- **移除 vestigial 表定义**:`cp_job_observations` 全仓库零读写(仅 `CREATE TABLE`),从 schema 中删除;已部署库中的空表无害留存,不做数据迁移。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `control-plane-reconciliation`:扩展「Reconciliation triggers and recovery」需求中的持久历史保留语句——保留策略从「operation 与 checkpoint attempt 记录」扩展为同时覆盖**已处理的 outbox 行**与**终态 attempt 记录**;新增两个 reclaim 场景(稳态 churn 下 outbox 与 attempts 收敛)。

## Impact

- `crates/arkflow-server/src/storage.rs`:两个新 prune 查询 + StorageActor 命令接线(模式照抄 `prune_operation_history`);删除 `cp_job_observations` 的 CREATE 语句。
- `crates/arkflow-server/src/hub.rs`:两个 Hub 级 `prune_*_history` 包装(镜像既有三个);`ControlPlaneStatus` 的 outbox 统计口径不变。
- `crates/arkflow-server/src/lib.rs`:从 reconcile tick 摘除四个 retention prune,新增 60s 维护任务。
- 无 schema 迁移(两个新查询作用于既有表;`cp_job_observations` 仅删 CREATE,存量库不受影响);无新依赖。
- 明确不在本 change:soak/规模验证设施(独立 verification change)、审计清扫查询重写(watermark 优化,非目标)、outbox age 告警语义、内存侧容量常量(`MAX_OPERATIONS` 等,已够用)。
