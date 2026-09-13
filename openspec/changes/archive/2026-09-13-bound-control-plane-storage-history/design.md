## Context

2026-09-13 代码侦察结论(为长稳验证立项做的压力点排查):

- `cp_outbox` 是 reconcile 的 transactional outbox:`INSERT` 于 intent 入队/恢复(storage.rs:1805、2091、2101),由 `claim_outbox` 认领(30s lease,storage.rs:1971)后置 `processed_at_ms`。**无任何 DELETE**——已处理行永久累积。
- `cp_attempts` 每次派发尝试一行(`state` 词表与 `cp_operations` 一致:queued/dispatched/acknowledged/running 为 active,其余终态;`cp_one_active_attempt` 唯一索引保证每 (node,stream,generation) 至多一条 active)。`expire_attempts` 只 UPDATE 状态,**无 DELETE**。
- `lib.rs:410-432` 的 reconcile tick(`max(poll_interval_ms, 50)` ms,默认 1s)每轮捆绑 10 项工作,其中 `prune_operation_history`、`prune_stale_checkpoint_records`、`prune_audit_history`、`prune_events(2048)` 是纯保留清扫(无行为语义);审计清扫含 `event_id NOT IN (SELECT … ORDER BY … LIMIT 100000)` 与 `occurred_at_ms < cutoff` 两个 DELETE。
- 既有可照抄的清扫模式:`prune_operation_history`(storage DELETE 双界 + Hub 内存侧同步收缩,`hub.rs:1637-1682`)、`prune_audit_history`(30d/100k)、`prune_stale_checkpoint_records`(24h + 内存)。
- StorageActor:mpsc + 单 `Mutex<Connection>`(WAL、synchronous NORMAL、busy_timeout 5s)——所有写串行,清扫成本直接挤占业务写预算。

约束:这是长稳验证的先导修理包,必须外科化;不能改变任何 reconcile 行为语义;不得引入 schema 迁移负担。

## Goals / Non-Goals

**Goals:**

- 两张无界表获得与 `cp_operations` 同构的双界保留,稳态 churn 下行数收敛。
- 保留清扫与 reconcile 延迟解耦:1s tick 上只剩行为性工作。
- 零 schema 迁移、零行为变更——soak 的「表行数收敛」断言从此全部可硬。

**Non-Goals:**

- soak/规模验证设施本身(后续独立 verification change)。
- 审计清扫的查询优化(如 processed-watermark 增量删除)——60s cadence 已把成本摊薄 60 倍,重写留给未来有证据时。
- 内存侧容量常量调整(`MAX_OPERATIONS=1024` 等已够用且不在本 change 焦点)。
- `cp_job_observations` 的历史数据迁移(表本就零读写,存量空表无害)。
- outbox age 告警指标口径变化。

## Decisions

### D1:双界参数与 `cp_operations` 完全对称(24h 窗口 / 4096 行)

outbox 已处理行与终态 attempt 行都按「更新/完成时间早于 24h 前删除,且终态行数超 4096 时按时间序裁到界内」清扫。理由:与 `prune_operation_history` 同构,运维心智一致;24h 覆盖「重放/排障需要最近一天历史」的调试窗口;4096 与 operations/events 一致,使三张历史表的稳态上界可加总估算。

被否决:给 outbox 单独配 `RETENTION_MAX=100_000`(audit 量级)——outbox 行是机械性中转记录,不是审计事实,4096 足够;attempt 行含 error 文本可能大,同样 4096。

### D2:清扫谓词

- outbox:`DELETE FROM cp_outbox WHERE processed_at_ms IS NOT NULL AND processed_at_ms < ?1`(窗口删)+ `… NOT IN (SELECT outbox_id FROM cp_outbox WHERE processed_at_ms IS NOT NULL ORDER BY processed_at_ms DESC LIMIT ?2)`(计数删)。**未处理行(含 claim lease 未释放的)永不清扫**——它们是待办。
- attempts:`state NOT IN ('queued','dispatched','acknowledged','running') AND finished_at_ms < ?1` + 同构计数界(以 `finished_at_ms DESC`,NULL finished 视作 created)。active 行受唯一索引与谓词双重保护;`expire_attempts` 标记的 expired 行在 24h 后回收。

### D3:cadence 拆分——独立 60s 维护任务,快 tick 只留行为 sweep

`lib.rs` 新增 maintenance task(`tokio::time::interval(60s)`,与既有 sweep/reconcile 任务同构),执行:两个新 prune + 既有三个 prune + `prune_events(2048)`。reconcile tick 移除这五项,保留 `expire_attempts`、`expire_stale_job_operations`、`schedule_periodic_checkpoints`、`reconcile_once/jobs/rollouts`——它们驱动重试/超时判定,必须保持 1s 粒度。

60s 的依据:所有保留窗口 ≥24h,60s 的回收延迟在语义上不可观测;清扫成本(尤其 audit 的 NOT IN)从每秒一次摊薄到每分钟一次,单写者预算归还业务写。固定常量而非配置项——没有已知的运维调参需求,引配置是过度设计。

被否决:把 prune 留在 tick 上但内部节流(记 last_run)——效果相同但把两类职责继续耦合在一个任务里,且任务 panic 域不隔离;独立任务更清晰。

### D4:vestigial `cp_job_observations` 直接删 CREATE

全仓库唯一引用是 schema 里的 `CREATE TABLE IF NOT EXISTS`(storage.rs:3336),无读写。删除定义对存量库零影响(表留在那里但不再被创建/使用),对新库消除一张幽灵表。执行前以 grep 复核零引用作为任务验收条件。

### D5:状态端点口径不变

`outbox_pending`/`outbox_claimed` 统计(`processed_at_ms IS NULL` 域)不受清扫影响——清扫只删已处理行,监控语义零变化。这一点写进测试断言。

## Risks / Trade-offs

- [清扫删除的已处理 outbox 行被外部依赖(如离线对账工具读取)] → 无已知读者(代码内 `claim_outbox` 只读未处理域);24h 窗口保留一天历史兜底;风险接受并记录。
- [60s cadence 下清扫执行瞬间可能与 reconcile tick 撞车,单写者上短暂排队] → busy_timeout 5s 足够吸收;清扫本身是 DELETE,行数受 4096 界约束,单次耗时可控。
- [`cp_one_active_attempt` 唯一索引与 attempt 清扫的交互] → 谓词只删终态行,active 行不动;测试断言 active 行在清扫后原样保留。
- [移除 `prune_events` 后 1s tick 不再每秒收 events] → events 表本来就有 24h/4096 双界,60s 内超界幅度 ≤ 每秒事件量 × 60,可忽略;测试覆盖。

## Migration Plan

纯增量:新查询作用于既有表,无 schema 变更(仅删一张幽灵表的 CREATE)。部署即生效;回滚即回到现状(无界增长恢复,但无数据损坏)。存量库中的 `cp_job_observations` 空表无需处理。

## Open Questions

- 无。参数(24h/4096/60s)如有异议在实现前提出,改动成本一行。
