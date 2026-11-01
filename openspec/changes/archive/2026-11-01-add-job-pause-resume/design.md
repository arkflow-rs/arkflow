# Design: add-job-pause-resume

## Context

Job 生命周期的现状（证据见 proposal）：

- 期望态是单个字符串：`JobRecord.desired_state`（`crates/arkflow-server/src/storage/mod.rs:339-351`），只能取 `running | stopped`，由 API 校验（`crates/arkflow-server/src/api/jobs.rs:999`、`:1025`）与协调器翻译（`crates/arkflow-server/src/hub/placement.rs:580-581`）。
- 停止 = 派发 `JobStop` → Agent 侧 teardown（`crates/arkflow-server/src/agent/kernel.rs`）；启动 = 派发 `JobStart`，对 durable 有状态 Job 要求存在兼容恢复工件（`crates/arkflow-server/src/hub/placement.rs:612-663` 的 recovery 守卫）。
- 恢复工件：`hub_job_recovery_artifact`（`crates/arkflow-server/src/api/jobs.rs:883`）先写 `JobCheckpointRecord{kind: checkpoint|savepoint, status: pending}`（`crates/arkflow-server/src/hub/checkpoint.rs:30`），完成由 Agent 报告聚合后写回 `checkpoint_id`（`crates/arkflow-server/src/hub/operations.rs:805-811`），checkpoint 保留清扫按 pin 过滤（`crates/arkflow-server/src/hub/checkpoint.rs:393-424`）。
- 观察上报是 expected-generation CAS（`crates/arkflow-server/src/hub/lifecycle.rs:15-90`）；收敛判定是 `desired_state == observed_state` 的字符串相等（同文件 `:27-32`）。
- 升级编排已验证了一条可复用的「崩溃可恢复的多相位操作」模式：持久化相位 + 相位守卫写 + 重放（`crates/arkflow-server/src/hub/job_orchestration.rs`，`openspec/specs/job-upgrade-orchestration/`）。

暂停的产品语义定调：**操作员要的是一个可恢复的「冻结」**——停下计算、不丢状态、之后能原样继续。而不是「停掉一个 Job 再祈祷操作员记得先打 savepoint」。

## Goals / Non-Goals

**Goals:**

- `paused` 成为一等的期望态：暂停 = 先固化状态（savepoint），再停机；恢复 = 自动从暂停工件起跑，操作员无需提供工件 id。
- 全程崩溃安全：Hub 重启/选主切换后暂停与恢复都能幂等续跑；不能出现「工件已生成但 Job 永远停不下来」「Job 停了但工件被 GC 或丢失指针」。
- 复用既有机制，Agent 协议与内核零改动；未被暂停的 Job 行为逐字节不变。
- 暂停相位可见、可审计、可观测（事件/指标/审计/Job 字段/Console）。

**Non-Goals:**

- 原地热暂停（kernel 常驻、只停读源、保留分区分配）——见 Non-goals，需要内核 pause 原语，另立专项。
- 自动恢复、暂停过期、暂停中升级/改并行度、单 task 暂停、Stream 级暂停、跨 Hub 新协议。

## Decisions

### D1 持久暂停（savepoint → stop），不是热暂停

内核当前只有取消/停止原语：chain 消费 `CancellationToken` 后退出并关闭资源（`crates/arkflow-core/src/executor/task.rs`、`stream-runtime-control` 的关闭顺序规格）。实现热暂停需要在 source chain 前插入 pause gate、让 barrier/checkpoint 在暂停期继续工作、并让 Agent 在重启后重建「暂停中的 kernel」——这是新的运行时原语，风险与工作量远超本 P2。

因此 v1 语义定为：**暂停 = 在暂停请求的 generation 上先做一轮 savepoint，完成后再停 kernel**。它给操作员带来的价值（释放计算资源、精确恢复、无需手动工件）与热暂停一致，代价是恢复时的重放窗口（见 D6）。热暂停作为后续专项，规格中显式声明边界。

### D2 `desired_state` 扩展 + 独立相位字段

`desired_state` 增加 `paused`；但不把「暂停进行到哪一步」编码进字符串，而是新增 `pause_phase`：

| 字段 | 取值 | 含义 |
| --- | --- | --- |
| `desired_state` | `running` / `stopped` / `paused` | 操作员意图 |
| `pause_phase` | NULL / `capturing` / `stopping` / `paused` / `resuming` | 暂停状态机相位（持久） |
| `pause_artifact_id` | Option<String> | 本次暂停冻结的 savepoint id |
| `pause_retry_count` | u32 | 崩溃/失败后的重派发次数（有界） |
| `paused_at_ms` / `paused_by` | Option | 暂停起点与发起人（resume 成功后清除） |

不变量：
- `pause_phase ∈ {capturing, stopping, paused}` ⇒ `desired_state = paused`。
- `pause_phase = resuming` ⇒ `desired_state = running` 且 `checkpoint_id`（恢复指针）指向暂停工件。
- `pause_phase = paused` 时才接受 resume；`pause_phase` 非空期间协调器围栏该 Job 的正常派发。
- 与升级编排一致，相位转移用「读当前相位 → 条件写」的守卫写（条件不匹配即本轮跳过，下个 tick 重试），避免与操作员动作/并发 tick 竞争。

### D3 复用既有工件命令与 stop/start，不新增 Agent 命令

暂停/恢复只改 Hub：暂停工件经既有 `record_job_checkpoint(kind=…)` 通道派发；stop 是既有 `JobStop`；resume 是既有 recovery `JobStart`（`checkpoint_id` 已指向恢复点即触发恢复）。Agent 侧只看到它已经支持的命令，`paused` 永不离开 Hub。

- **理由**：Agent/内核改动面为零（`job_savepoint` 要求目标节点上 Job 正在运行，`agent/kernel.rs:714-721`，所以顺序必须是「工件完成 → stop」），暂停的失败模式与工件/stop 完全同构，测试可复用既有 harness（`two_node_job_smoke.rs` 等）。
- **工件 kind 由 Job 的恢复策略决定**（这是本设计最关键的约束）：恢复选择在 `hub/placement.rs:1024-1055` 先按 `spec.recovery` 过滤 kind（`LatestCheckpoint→checkpoint`、`LatestSavepoint→savepoint`、`Fail→无`），再按 `created_at` 取最新 `completed`。因此：
  - `latest_checkpoint`（默认）→ 暂停工件用 `kind="checkpoint"`（retention 会回收 completed checkpoint，必须 pin）；
  - `latest_savepoint` → 用 `kind="savepoint"`（retention 不回收 savepoint，pin 仅防御）；
  - `recovery: fail` → 无任何候选，resume 必然 fail-closed（`placement.rs:1058-1063`），故暂停入口直接拒绝（`pause_requires_recovery_policy`）。
  这样 resume 不需要临时改写 `spec.recovery`（升级编排的做法，`api/jobs.rs:570`、`hub/job_orchestration.rs:130`），也不需要重写版本历史行。
- **代价**：暂停不是「瞬时」的（要等一轮工件 + teardown），所以 API 返回 202 + 相位可观测，而不是同步返回 200。

### D4 相位推进由协调 tick 驱动；操作员动作只写意图

- 暂停请求（handler）只做前置校验 + 一次 generation-fenced 写（`desired_state=paused`、`pause_phase=capturing`、`pause_artifact_id`、`pause_prev_checkpoint_id`、`paused_*`、bump generation），随后立即派发工件（若 Job 需要且可做工件）。
- 相位推进放在协调 tick 的独立 `reconcile_job_pauses()`（与 `reconcile_jobs()`/`reconcile_job_upgrades()` 并列，leader-only），规则：
  - `capturing`：确保工件命令在途；若 `JobCheckpointRecord(status=completed)` 出现 → 写 `stopping` 并派发 `JobStop`；若记录失败或相位 deadline 超时 → 按 D5 中止。
  - `stopping`：派发/重派发 `JobStop`；当全部目标节点观察为 `stopped` 且 generation 匹配 → 写 `paused`；目标节点离线时保持相位与 `reconciling`，不误判收敛。
  - `paused`：不派发任何命令（Job 已是 stopped），只维持 pin。
  - `resuming`：由通用协调器的 recovery start 推进；观察到 `running` 且收敛 → 清除相位与 `paused_*`（解除 pin）。
- **理由**：把「多步、可能失败、需要重放」的逻辑放在 tick 里，与既有升级编排同构；handler 保持薄，避免长事务。
- **alternatives**：在 handler 内 await 工件完成（同步语义更直观）——但工件可能耗时数分钟，handler 超时/重试与 Hub 重启语义都会变复杂，且与既有异步工件模型不一致。

### D5 失败与崩溃语义

| 事件 | 行为 |
| --- | --- |
| 工件命令失败（Agent 报告 failed） | 中止暂停：`desired_state=running`、`pause_phase=NULL`、清空暂停字段；**把 `checkpoint_id` 回滚到 `pause_prev_checkpoint_id`**（`record_job_checkpoint` 在触发时已前移指针，失败不回滚，`hub/checkpoint.rs:49-56`、`:214-290`）；旧 generation 仍在运行；写 `last_error`、SSE `job.pause_failed`、审计 `job.pause` 终态 `failed` |
| 工件相位超时（deadline，默认 5 分钟） | 同上（先按失败记录；不静默重试） |
| Hub 重启且 `capturing` 在途 | 既有操作恢复把在途命令结算为 TimedOut；协调 tick 用新工件 id 重派发一轮工件（`pause_retry_count+1`）；超过上限（3）中止并按上一行回滚指针 |
| Hub 重启且 `stopping` | tick 重新派发 `JobStop`（幂等；Agent 对已停 Job 的 stop 是 no-op，`agent/kernel.rs:1044-1057`） |
| 目标节点离线 | 保持在 `stopping` 相位与 `reconciling`，不误判 converged；节点回归后继续派发；`expire_stale_job_operations` 的重试耗尽不会把暂停判成失败（暂停相位有自己的重派发预算与围栏），操作员可 `stop` 强制落回 stopped |
| resume 启动失败 | 不自动回滚到 paused：Job 进入既有失败语义（`last_error` + 后续协调重试），相位保持 `resuming` 且工件保持 pin，操作员可再次 resume 或 stop |
| 操作员在暂停进行中发 stop | 接受：`desired_state=stopped`、清除暂停相位与 pin（工件可被保留期正常回收） |
| 操作员在暂停进行中发 start/restart | 等价 resume：`desired_state=running`、相位转 `resuming`，恢复指针保持 |
| 操作员在暂停进行中再次 pause | 409 `pause_in_progress`（已 paused 则 `job_already_paused`） |

所有相位写都带条件守卫（期望相位 + 期望 generation 或期望 checkpoint 状态），条件不满足即跳过，保证多 tick/多操作员竞争下不会双写。

### D6 恢复语义与 replay 窗口（诚实边界）

- 暂停工件与手动 checkpoint/savepoint 同构：源位点（Kafka offset 等）与 keyed state 一起落入工件；resume 从该 cut 恢复。
- 事务型 sink（Kafka L3、SQL 单事务批）保持 exactly-once 语义；非事务 sink 在「工件 barrier 之后到 teardown 完成」之间处理过的数据会在 resume 时重放——窗口与原子升级的已声明语义一致（`job-upgrade-orchestration`「Atomic upgrade cutover semantics SHALL be explicit」）。文档必须写明，不能宣传「零重复暂停」。
- **未声明 checkpoint 配置的 Job**：没有 durable 恢复工件可做（`spec.checkpoint.is_none()`），暂停跳过工件相位直接 stop，resume 等价冷启动；文档明示该差异。声明了 durable state 但缺 checkpoint 配置的配置组合本就被既有恢复守卫拒绝，不在本变更扩面。
- **`recovery: fail`**：无法选择任何工件，暂停入口拒绝（`pause_requires_recovery_policy`），文档说明先用恢复策略重建/升级 Job。

### D7 保留期 pin

`enforce_checkpoint_retention_for_job`（`crates/arkflow-server/src/hub/checkpoint.rs:466`）目前按 `pinned_job_upgrade_savepoints` 过滤 pinned（`:419-424`），且只对 `kind=="checkpoint"` 的 completed 记录做回收决策（`:409-411`）。新增 `pinned_job_pause_artifacts(job_id)`：当 `pause_phase != NULL` 时返回 `pause_artifact_id` 与当前 `checkpoint_id`（工件重试会换 id，两者可能短暂不同），并入同一过滤集合。resume 收敛（相位清除）、stop、delete 后自然解除。对 savepoint kind 的暂停工件，pin 只是防御性（savepoint 不进回收候选）。

### D8 收敛判定

`observe_job` 目前 `converged ⇔ desired_state == observed_state`（`hub/lifecycle.rs:27-32`）。新增：`desired_state=paused` 时，仅当 `pause_phase=paused` 且观察为 `stopped` 才判定收敛；相位未到 `paused`（capturing/stopping）一律 `reconciling`。Agent 观测值域保持 `stopped|failed`（`agent/session.rs:417-447`），不新增 `paused` 观测值。实现为一个小助手函数，单测钉死「paused + stopped + 相位未完成 → reconciling」「paused + stopped + 相位 paused → converged」。

### D9 API 形状

- `POST /api/v1/jobs/{id}/actions/pause` → `202` + 最新 `JobRecord`（含 `pause_phase`）；前置条件不满足 `409` + 稳定 problem code（`job_not_running` / `pause_in_progress` / `job_already_paused` / `orchestration_in_progress` / `pause_requires_recovery_policy`）。
- `POST /api/v1/jobs/{id}/actions/resume` → `202` + `JobRecord`；仅 `pause_phase=paused` 接受，否则 `409 pause_not_paused`。
- `PUT /api/v1/jobs/{id}/desired_state {"state":"paused"}` 与 pause 等价；`{"state":"running"}` 在暂停 Job 上等价 resume。
- `GET /api/v1/jobs...` 响应 additive 增加 `pause_phase`、`pause_artifact_id`、`paused_at_ms`、`paused_by`。
- 授权：pause/resume 需要 `Operate` 动作（与 start/stop 同级）；审计记录 actor/action/target/outcome/失败码，不含 spec 与凭据。
- 错误码表写进 `reference/api.md`：`job_not_running`、`pause_in_progress`、`job_already_paused`、`pause_not_paused`、`job_paused`（升级被拒）、`pause_savepoint_failed`、`pause_requires_recovery_policy`、`orchestration_in_progress`。
- **既有测试期望需同步更新**（不是回归）：`api/tests.rs:3448-3510` 现在断言 `actions/pause → 400 invalid_job_action`，`api/tests.rs:3698-3800` 断言创建 `desired_state:"paused" → 400`（后者保持），新增 positive/negative 路由测试替换旧断言；wire 协议字符串往返测试（`hub/wire.rs:404-437`）保持绿（未新增命令）。

## Risks / Trade-offs

- **`desired_state` 字符串扩散**：新增第三值后，任何 `match desired_state` 的旧代码都可能落入未覆盖分支。缓解：`rg desired_state` 全量审计（storage 读写、协调器、升级编排、rollout、Console、测试），把「未知值」从静默跳过改为显式非法值分支；新增跨切面单测遍历三值。
- **工件 kind 与恢复策略漂移（最高风险）**：若暂停工件 kind 与 `spec.recovery` 不匹配，resume 会在工件选择处 fail-closed 且错误被 `reconcile_jobs` 吞成 warn，表现为「Job 永远起不来」。缓解：kind 由策略推导（D3）、`recovery: fail` 拒绝暂停、测试覆盖两策略、resume 缺工件时必须 409/明确告警而不是静默卡住。
- **指针污染**：`record_job_checkpoint` 在触发时前移指针且失败不回滚；暂停中止若不恢复 `pause_prev_checkpoint_id`，后续崩溃恢复会被钉在 failed 工件上。缓解：abort 路径回滚指针 + 专用测试（失败后模拟重启并再次 start）。
- **阶段间崩溃窗口**：工件完成与写 `stopping` 之间、写 `stopping` 与派发 stop 之间、resume 收敛与清相位之间都可能崩溃。依赖相位持久 + tick 重放；测试用「完成后立即重开 Hub」注入。
- **paused 语义被误当 stopped**：升级的 stopped 模式前置条件（`desired_state != "stopped"`）会让暂停 Job 通过；必须在升级入口显式拒绝 `pause_phase != NULL`（含 resuming），并有测试。
- **离线节点**：stop 派发不到离线节点，暂停长期 `reconciling` 是正确语义（intent 持久），但需要 UI 文案与「操作员可 stop 强制落回」的出口，否则会被当成卡死。
- **暂停中的操作员动作竞争**：generation fence 保证「谁先写谁赢」，但必须保证落败方拿到 409 而非静默覆盖；新增并发测试（pause vs resume vs stop vs upgrade）。
- **工件成本**：暂停对大状态 Job 可能耗时（一轮 barrier + 状态上传）。相位 deadline 与失败语义保证不会永远挂起；API 202 + 相位可见让操作员知道进度。
- **既有断言与文档漂移**：`api/tests.rs:3448-3510` 当前断言 `actions/pause → 400`，实现后必须改写为矩阵测试；`docs/reference/api.md` 关于 `If-Match`/`Idempotency-Key` 与「等价于 stream actions」的表述本就不符实现，顺带纠偏（tasks 6.3）。
- **HA 选主**：暂停状态机只在 leader 的协调 tick 推进（沿用现有 leader-only reconcile），新 leader 从持久相位续跑；无需新协议，但需要一条「选主后暂停续跑」的测试（复用 `hub_lease_election` 风格的 harness）。

## Migration Plan

1. **存储**：`SCHEMA_VERSION` v3 → v4；SQLite `CREATE TABLE` 更新（`crates/arkflow-server/src/storage/sqlite.rs:2595`）+ 打开时 `ALTER TABLE cp_jobs ADD COLUMN`（既有探测式迁移块 `:2891-2975`）；PostgreSQL DDL 更新（`postgres.rs:694` 区）+ `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`（`:736` 模式）；同步检查 `backup_restore.rs`（`INSERT INTO cp_jobs` 手写列清单 `:804`）、`table_copier.rs`/`migrate_tool.rs` 中构造 `JobRecord` 的位置（`table_copier.rs:969`、`migrate_tool.rs:69`）。旧库打开即补齐，新列 NULL/0 语义 = 未暂停。
2. **读路径**：`JobRecord` 新字段 `Option`/`Default`，旧行读出为未暂停；所有 SELECT/INSERT 列表同步（SQLite 与 PG 方法各一处）。
3. **兼容性**：旧二进制读新库被 `SchemaTooNew` 拒绝（既有语义）；新二进制读旧库自动迁移（既有语义）。备份/恢复工具随 schema 自动携带新列（`TABLE_ORDER` 不新增表）。
4. **回滚**：无破坏性 DDL；回滚到旧二进制需库仍为 v3 → 生产回滚需先恢复备份，文档写明（与既往 schema 升级一致）。
