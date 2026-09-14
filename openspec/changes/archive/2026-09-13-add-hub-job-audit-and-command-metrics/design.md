## Context

Hub 的生产化收尾缺口(PLANNING 5.2-9,2026-09-13 核实):Job 操作无审计(偏离 `control-plane-identity` Actor-aware audit)、`/metrics` 无命令延迟/失败率直方图、Job 命令缺过期/重试元数据(弱于 stream intents 的 durable 模型)。附带发现:`cp_audit_events` 无清理路径,不满足 `control-plane-fleet` 的 bounded retention 要求,纳入本次一并收口。

现有底座(直接复用,不重建):

- 审计:`cp_audit_events` 表 + `Hub::record_audit_event`(`hub.rs:3361`)+ 查询 API;stream/rollout/maintenance 已有调用点。
- 指标:`/metrics` 为手写 Prometheus 文本(`lib.rs:2270` `hub_metrics`),非 prometheus crate registry;数据来自 `operational_status()`。
- 操作:`cp_operations`(operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json)+ 周期 sweep(`storage.rs:2121-2138`)。
- Job 派发的 terminal-state skip 与 fencing 已由 `repair-v1-review-defects` 落地(hub.rs dispatch ~788-837)。

## Goals / Non-Goals

**Goals:**

- Job 生命周期 mutation(start/stop/checkpoint/恢复)在 Hub 接受或拒绝时落审计,字段与现有审计一致。
- `/metrics` 新增:按 (command_type) 的 dispatch→ack 延迟分桶计数、按 (command_type, outcome_class) 的成败计数。
- `cp_operations` 为 Job 操作补 `expires_at_ms`/`retry_count`,过期进入可重试语义,重试有上限。
- `cp_audit_events` 接入既有周期 sweep 的有界保留。

**Non-Goals:**

- Session token 轮换、RBAC、labels/批量操作、长稳验证(见 proposal Non-goals)。
- 数据面内核指标;stream intents 的 durable 模型不动。
- 不把 `/metrics` 迁移到 prometheus crate registry(纯重构,无行为收益,另立)。

## Decisions

### D1 — 审计记录点:Hub 接受 mutation 处,与 stream 审计同层

在 Hub 的 Job mutation 入口(接受/拒绝分支)调用 `record_audit_event`,覆盖代码真实操作词表:`job_start`/`job_stop`/`job_checkpoint`/`job_savepoint`(hub.rs:2788、agent.rs:1632);审计 action 采用现有点分隔风格映射为 `job.start`/`job.stop`/`job.checkpoint`/`job.savepoint`,resource_type=`job`。拒绝路径(鉴权失败、Capacity、fencing 拒绝)同样落审计,outcome 与 failure_code 区分。

- **恢复不单独立审计点**:recovery 是 job_start 的 `RecoveryPolicy`(hub.rs:804),随 start 的审计记录携带,不虚构独立操作。
- **`*_commit` 操作不在本范围**:`job_checkpoint_commit`/`job_savepoint_commit` 是 Agent 上报的命令结果而非 operator mutation,经由既有 `command_result`(hub.rs:2700)路径,是否留痕由 `command.dispatch` 审计覆盖,不新增审计点。
- 备选:在 Agent 侧上报时审计 → 拒绝。审计必须覆盖"从未到达 Agent"的 mutation(Capacity 拒绝、离线节点),Hub 是唯一权威点;`control-plane-identity` 要求 accepted **or rejected** 都记录。
- 备选:在 storage actor 层拦截 → 拒绝。会把业务语义(action 命名)下沉到存储层,且拒绝路径不经过 storage 写入。

### D2 — 审计 message 脱敏:message 只含操作元数据,不含 config/计划体

Job 操作的 `operation_json` 可能含完整 JobSpec(含连接串等)。审计记录的 message 字段只写操作摘要(generation、operation、目标节点数、失败类别等标量),**不复制 operation_json**;凭证与配置值的排除由 identity spec 的 MUST NOT 约束兜底。实现时以一条"审计 message 不含 config 内容"的单测钉死(参照 `command.dispatch` 现有写法核实其 message 内容,若已含敏感体则一并收敛)。

### D3 — 延迟指标:内存累积器 + 手写文本导出,测 enqueue→ack

在 Hub 持有 per-command-type 的原子累积器(延迟分桶计数 + 成败计数),在操作状态迁移到 acknowledged/terminal 时更新;`hub_metrics` 渲染为 `arkflow_command_duration_bucket{command=...,le=...}` 计数与 `arkflow_command_total{command=...,outcome=...}`。延迟定义取 `created_at_ms` → ack/terminal 时刻(数据已在内存与 `cp_operations`,免解析 operation_json)。

- 备选:scrape 时从 `cp_operations` 现算 → 拒绝。每次 scrape 全表聚合,且保留窗口外的历史不可见。
- 备选:prometheus crate registry + histogram → 拒绝(本次)。与现有手写文本导出并存两套机制,重构价值不足;分桶计数器在文本层面与 histogram 语义等价。
- 重启清零:累积器为进程内存,重启后计数归零——符合 Prometheus counter 语义,spec 场景按"重启后重新累积"表述,不做持久化聚合(避免每次 ack 多一次 SQLite 写)。

### D4 — 分桶与标签:固定命令类型枚举 + 固定桶,守住低基数

command 标签取固定枚举(`job_start`/`job_stop`/`job_checkpoint`/`job_savepoint`/`stream_start`/`stream_stop`/`restart`/`apply_configuration`/`agent` 派发类),桶取固定毫秒档(5/10/25/50/100/250/500/1000/2500/5000/10000/+Inf);outcome_class 取 `ok`/`rejected`/`node_unavailable`/`capacity`/`timeout`/`failed` 固定枚举。resource_id/correlation_id/错误文本不入标签(observability spec 既有约束)。

### D5 — Job 操作过期/重试:serde 默认字段 + 复用周期 sweep(实现修订)

实现时发现比 ALTER TABLE 更干净:Hub 恢复时从 `operation_json` 反序列化整个 `HubOperation`(hub.rs `list_operations` → `from_str::<HubOperation>`),因此 `expires_at_ms` 只需作为 `#[serde(default)]` 结构体字段加入,旧行反序列化为 `None`(永不过期 = 行为不变),**零 schema 迁移**;`retry_count` 字段本就存在。过期语义:周期 sweep(`expire_stale_job_operations`)把过期 Queued/Dispatched 的 Job 操作置为 terminal `TimedOut`(failure_class=`expired`,retry_count+1),reconcile 下一 tick 自然重入队;重入队继承同 (node, resource, operation, generation) 的最大 retry_count;达到 `MAX_JOB_OPERATION_RETRIES`(常量 3,沿用 RETENTION_MS 常量先例而非新配置项)→ terminal `Failed`/`expired`,再次入队被拒。sweep 同时摘除节点队列/租约中的命令,防止迟到轮询执行僵尸命令;锁序(nodes 先于 operations)与 enqueue 保持一致避免死锁。

- 备选:过期即删除 → 拒绝。`control-plane-hub` dispatch 要求明文禁止:"Expired leased commands SHALL … not disappear while their operation remains an active deduplication record"。
- 备选:ALTER TABLE 加列 → 放弃。列在 operation_json 之外无独立查询需求,serde 默认字段达成同兼容保证且零迁移。

### D6 — 审计保留:接入既有 sweep,按条数 + 时间双界

周期 sweep 增加 `cp_audit_events` 清理:超过保留时间(常量 30 天)且超出条数上限(常量 100k)的旧行删除,与 `cp_operations` 清理同一节拍;常量先例与 `prune_operation_history` 一致(后者同样用常量非配置);满足 `control-plane-fleet` bounded retention 要求,API 行为不变(查询仍按 event_id DESC LIMIT 1024)。

## Risks / Trade-offs

- [checkpoint 触发频繁导致审计膨胀] → checkpoint 触发属 Job mutation 但频率可能高于 start/stop;审计粒度定为"操作接受"而非"每个 checkpoint 完成",保留策略(D6)兜底;若实现中实测量级过高,checkpoint 周期触发降为聚合审计(单测留钩子),在 tasks 中显式验证。
- [内存累积器在 Hub 重启后丢失延迟历史] → 接受(Prometheus 常规语义);需要跨重启的延迟分析走日志/审计时间戳。
- [ALTER TABLE 在大库上的锁表] → SQLite ADD COLUMN 为元数据操作,量级可忽略;迁移沿用既有 PRAGMA 版本检查模式。
- [审计调用点遗漏未来的新 Job 操作] → tasks 中以"mutation 入口清单核对"收尾,并在 `api_contract.rs` 契约测试处补"每个 mutating route 有审计断言"的防回归检查(可行则做,不可行则在 design 记录原因)。

## Migration Plan

SQLite 追加式迁移(PRAGMA 版本 +1),旧库自动补列;`/metrics` 只增指标;审计为新写入,无回填。回滚 = 旧二进制忽略新列与新指标,无数据破坏。部署顺序无要求(单二进制)。

## Open Questions

- ~~`command.dispatch` 现有审计(`hub.rs:2540`)的 message 是否已含未脱敏内容~~ → **已解决(任务 1.1)**:全部 6 个既有审计点的 message 均为标量元数据(操作名、节点、状态迁移),无脱敏问题;新测试 `job_start_is_audited_without_configuration_bodies` 以注入 `connection_string` 的 spec 钉住该约束。

## Implementation Deviations (2026-09-13)

- D3 补充:outcome 类实际枚举为 `enqueued`/`acknowledged`/`succeeded`/`failed`/`timed_out`/`node_unavailable`/`capacity`/`rejected`(另含 `cancelled`/`superseded`),固定低基数;ack 分支同时记延迟桶与 `acknowledged` 计数。
- 6.1 可行且已实现:lib.rs 路由级测试 `job_action_route_leaves_an_audit_trail` 断言 POST `/jobs/{id}/actions/start` 后 `GET /api/v1/audit?resource_id=` 可见 `job.start`。
- 任务 4.1/4.2 的 ALTER TABLE/配置项表述由 D5/D6 修订版取代(serde 字段 + 常量)。
- **CR 修订(自审查)**:
  - 接受审计只在每个 (resource, operation, generation) 的**首次派发**写一条——reconciler 重派发是机制不是新 mutation,否则持续失败的 Job 每 tick 刷一条审计;指标仍计每次派发(rejection 审计不去重:节点不可用/容量拒绝是稀疏事件,且本身值得留痕);
  - 过期 sweep 只覆盖 `job_start`/`job_stop`(期望状态由 reconcile 重入队兜底);checkpoint/savepoint 触发的重试留在 poll 端既有 `expired_retries` 路径——sweep 若抢先置 TimedOut 并摘除命令,该路径将失去重放 payload 的机会,导致触发丢失;
  - 重试继承与上限显式限定 `job_*` 操作,stream attempt 的重试语义不经过此机制。
- **CR2 修订(第二轮自审查)**:
  - **指标双计数**:capacity 与 retry-budget 拒绝分支各自 `record_outcome` 一次、`reject_enqueue` 内又记一次 → 删除分支内的重复调用,统一由 `reject_enqueue` 记账;
  - **checkpoint 接受审计错位**:`schedule_periodic_checkpoints` 与 operator 触发共用 dispatch 漏斗,enqueue 级接受审计会把周期调度机制记成 operator mutation。接受审计收窄到 `job_start`/`job_stop`(dispatch 级);checkpoint/savepoint 的接受审计移到 HTTP 触发 handler(`hub_job_recovery_artifact`)——只有那里是真正的 operator mutation,且顺带修复了"零节点派发时触发无审计"的边角;调度器路径由测试 `periodic_checkpoint_scheduling_writes_no_audit_rows` 钉死为零审计;
  - **文档注释粘连**:插入新方法时截断了 `prune_operation_history` 的 doc block,后半段错误挂到 `command_metrics` 上,已修复归属;
  - 已知限制(记录在案):审计 actor 为静态 `"operator"`——JobRecord 不携带 actor,principal 贯穿需待 RBAC/多用户(`control-plane-identity` 已有目标 spec);checkpoint 触发若零节点派发,审计只有触发一条、无 per-node 拒绝记录。
