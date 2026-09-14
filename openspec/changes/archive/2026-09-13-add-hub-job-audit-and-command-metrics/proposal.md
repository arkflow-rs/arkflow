## Why

Hub 生产基础(PLANNING 5.2-9,2026-09-13 代码核实)存在三项收尾缺口,其中一项是**对现有 spec 的偏离**:

1. **Job 操作零审计,违反 `control-plane-identity` 的 Actor-aware audit 要求**("Every accepted or rejected control-plane mutation SHALL record the principal, action, target resource, correlation ID, outcome, and stable failure code")。现有审计点仅覆盖 `agent.register`(`hub.rs:1824`)、`command.dispatch`(`hub.rs:2540`)、`rollout.create`(`hub.rs:3434`,`hub.rs:3501`)、stream 操作(`storage.rs:1783`)与 `node.maintenance`(`storage.rs:1882`);v1 新增的整个分布式 Job 操作面(job 启动/停止/checkpoint 触发/恢复)没有任何审计记录——而这恰是生产环境最需要留痕的变更类操作。
2. **命令延迟与失败率直方图缺失**:`/metrics`(`lib.rs:2270` `hub_metrics`)只导出计数器与 gauge(reconciliation 计数、outbox、状态分布、rollout 状态、节点透传),没有 dispatch→ack 延迟分布,也没有按命令类型的失败率——运维排障最需要的两个信号。PLANNING 5.5 原文明确要求"命令延迟、失败率"。
3. **Job 命令幂等元数据弱于 stream intents**:stream 侧 durable intent 带 `idempotency_key`/`retry_count`/`next_retry_at_ms`/`expires_at_ms`(`storage.rs:1664`、`storage.rs:752`);Job 操作仅有 `operation_id` + terminal-state skip(`cp_operations`),无过期时间与重试上限。`control-plane-hub` 的 "Command idempotency and reconciliation" 要求未对 Job 命令给出同等保障。

## What Changes

- **Job 操作审计**:Job 生命周期变更操作(`job_start`/`job_stop`/`job_checkpoint`/`job_savepoint` 的接受与拒绝;恢复随 start 携带,不单独立点)在 `cp_audit_events` 落审计记录,字段对齐现有审计(actor、action、resource_type、resource_id、node_id、correlation_id、outcome、failure_code、message),并遵守"不含凭证与未脱敏配置"约束;审计查询 API 可按 resource_id 过滤到 Job。
- **命令延迟/失败率指标**:`/metrics` 新增低基数的命令延迟观测(dispatch→ack,按命令类型聚合的直方图或分桶计数)与按命令类型/结果分类的失败计数;遵守 observability spec 的"Resource IDs、correlation IDs、错误信息不得作为 metric label"约束。
- **Job 命令幂等元数据**:`cp_operations` 为 Job 操作补充过期时间与重试上限语义,对齐 stream intents 的 durable 模型;过期操作进入可重试/可再入队状态,而不是无限期滞留。

## Capabilities

### New Capabilities

(无——全部为既有 capability 的要求修订)

### Modified Capabilities

- `control-plane-identity`:Actor-aware audit 要求下新增显式场景——Job 生命周期 mutation(启动/停止/checkpoint/恢复)被审计记录,被拒的 Job mutation 同样留痕。
- `control-plane-observability`:Prometheus metrics 要求下新增命令延迟分布与按命令类型失败计数的要求与场景(低基数标签、不含资源 ID/错误信息)。
- `control-plane-hub`:Command idempotency and reconciliation 要求下扩展 Job 操作的过期与重试上限语义,对齐 stream intents 的 durable 行为。

## Impact

- `crates/arkflow-server/src/hub.rs`:Job 派发/终止/checkpoint 路径补审计调用;命令生命周期打点(入队/派发/ack 时刻已有 `cp_operations` 时间戳,新增聚合)。
- `crates/arkflow-server/src/storage.rs`:`cp_operations` 过期/重试元数据列与查询;审计写入复用现有 `cp_audit_events` 表(可能补 job 相关索引)。
- `crates/arkflow-server/src/lib.rs`:`hub_metrics` 导出新增指标。
- `crates/arkflow-server/tests/`:job 审计、指标输出、过期/重试行为的回归测试。
- 无破坏性 API 变更;`/metrics` 只增不改;SQLite schema 为追加式(`ALTER TABLE ADD COLUMN`,已有 migration 先例 `storage.rs:3549-3556`)。

## Non-goals

- Session token 短期化/轮换(涉协议演进,独立 change,见 PLANNING 5.2-9)。
- RBAC / 多用户 / 细粒度权限(阶段 4,`control-plane-identity` 已有目标 spec)。
- 节点 labels、按标签批量操作、配置审批流(阶段 3)。
- 长稳与规模上限验证(验证类工作,单独安排)。
- 数据面(内核)指标——已有 `control-plane-observability` 控制面范围与内核 `metrics.rs`,本次只补 Hub 命令面。
