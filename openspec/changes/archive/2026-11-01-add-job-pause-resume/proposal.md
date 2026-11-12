# Proposal: add-job-pause-resume（Job 级暂停/恢复）

## Why

分布式 Job 目前只有「运行 / 停止」两态，**没有可恢复的暂停语义**：

- `desired_state` 只有 `running | stopped` 二值，且**存储层不校验、协调器对未知值静默跳过**：创建入口校验 `crates/arkflow-server/src/api/jobs.rs:999`、desired-state 入口校验 `:1043`、写路径 `hub.update_job_desired_state`（`api/jobs.rs:1067-1070`）、动作映射 `start|restart → running`、`stop → stopped`（`api/jobs.rs:86`，映射见 `:105-118`）；API DTO `CreateJobRequest.desired_state` 在 `crates/arkflow-server/src/api_contract.rs:240`、`JobDesiredStateRequest` 在 `:279-281`。reconciler 只把这两个值翻译成 `JobStart` / `JobStop` 命令，其余值直接 `return Ok(0)`（`crates/arkflow-server/src/hub/placement.rs:579-583`）——新增 `paused` 若不同步改这三处，会得到一个「永不派发也不报错」的僵尸状态；`upsert_job` 也只在 `desired_state != "stopped"` 时触发协调（`crates/arkflow-server/src/hub/jobs.rs:82-84`）。
- **`stop` 只能回到「最后一个 completed 工件」，不能冻结在停机瞬间**：stop 只是把 desired 改成 stopped 后派发 `JobStop`，内核 cancel + close（`crates/arkflow-server/src/agent/kernel.rs:1044-1068`），**不停机前快照**；恢复时 reconciler 按 `spec.recovery` 过滤 kind、按 `created_at` 取最新 completed 工件（`crates/arkflow-server/src/hub/placement.rs:1024-1055`），无兼容工件则 fail-closed（`:1058-1063`，错误被 `reconcile_jobs` 吞成 warn）。而任何 desired 写入都会 bump generation（`crates/arkflow-server/src/storage/sqlite.rs:2471-2497`），Agent 的 state 目录按 generation 分区（`crates/arkflow-server/src/agent/kernel.rs:394-397`），所以 stop→start 一定是从旧工件回填的新跑：checkpoint 之后的状态与进度全部丢弃，且「指针指向未完成/失败工件」时连旧工件都选不中。操作员没有一个原子语义表达「此刻暂停、稍后从此刻继续」。
- **暂停语义只存在于升级编排内部**：`pause` 动作冻结的是编排相位（`crates/arkflow-server/src/hub/job_orchestration.rs:264-374` 的 `paused_from`/`phase=paused`，旧 generation 继续处理；`openspec/specs/job-upgrade-orchestration/spec.md:106-107`），由编排 tick 直接 `Ok(())` 停止推进（`:397`）；Job 自身没有任何等价能力，且暂停的编排并不暂停数据处理。
- **工件 kind 与恢复策略是硬耦合**：默认 `recovery: latest_checkpoint` 只接受 `kind == "checkpoint"`（`crates/arkflow-server/src/hub/placement.rs:1037-1041`）；升级编排靠把 `spec.recovery` 改成 `LatestSavepoint` 规避（`api/jobs.rs:570`、`hub/job_orchestration.rs:130`）。**暂停不得改写 spec/version**，因此暂停工件的 kind 必须跟随 Job 既有的恢复策略（见 What Changes），`recovery: fail` 的 Job 无法 resume，须在暂停入口拒绝。
- **恢复指针在触发时即前移、失败不回滚**：`record_job_checkpoint` 在版本匹配时立刻把 `cp_jobs.checkpoint_id` 指向新记录（`crates/arkflow-server/src/hub/checkpoint.rs:49-56`），失败只改记录状态（`:214-290`）。暂停的 savepoint 一旦失败，指针就钉在 failed 工件上，之后任何需要恢复的 start 都会 fail-closed——暂停状态机必须在失败时把指针回滚到暂停前的工件。
- **保留期只回收 checkpoint**：`enforce_checkpoint_retention` 只筛 `kind == "checkpoint"` 的 completed 记录（`crates/arkflow-server/src/hub/checkpoint.rs:409-411`），savepoint 从不被 retention 删除；pin 机制只对 checkpoint 生效（`:419-424`，pin 来源目前仅升级编排 `hub/job_orchestration.rs:246-258`）。因此暂停工件若为 checkpoint 必须接入 pin；若为 savepoint 则 pin 是防御性设计。
- 控制面已有可复用的机关：savepoint 命令与完成聚合、stop/start 的 generation 围栏与 dispatch-dedupe、`observe_job` 的 expected-generation CAS（`crates/arkflow-server/src/hub/lifecycle.rs:15-90`）、SSE 事件与审计漏斗（`crates/arkflow-server/src/hub/observability.rs:281`）、升级编排的崩溃恢复模式（`openspec/specs/job-upgrade-orchestration/`）。

`openspec/PLANNING.md` 第十一节 11.2-7 与 11.4 的 P2 行把该能力列为缺口（“`desired_state` 扩 pause + 恢复语义；与升级编排 pause 语义对齐”）。本变更交付**持久暂停**（durable quiesce）：先把当前状态固化成 savepoint，再停机；恢复时自动从该工件起跑。

## What Changes

- **`desired_state` 扩展为 `running | stopped | paused`**，新增持久化的暂停相位：`pause_phase ∈ {NULL, capturing, stopping, paused, resuming}`（`cp_jobs` 增列，schema v3→v4，SQLite/PostgreSQL 双后端 + 既有打开时迁移路径），另存 `pause_artifact_id`、`pause_retry_count`、`paused_at_ms`、`paused_by`。
- **暂停工件 kind 跟随 Job 的恢复策略（不改写 spec/version）**：`spec.recovery = latest_checkpoint`（默认）时暂停工件用 `kind="checkpoint"`（并接入保留期 pin）；`latest_savepoint` 时用 `kind="savepoint"`（retention 本就不回收 savepoint，pin 为防御性）；`recovery = fail` 的 Job 无法被恢复选工件，暂停入口直接拒绝（稳定码 `pause_requires_recovery_policy`）。这样 resume 走既有工件选择路径（`hub/placement.rs:1024-1055`）即可命中暂停工件，无需临时改写 spec/recovery 或版本历史。
- **暂停状态机（Hub 协调 tick 驱动，复用既有命令路径）**：
  1. `POST /api/v1/jobs/{id}/actions/pause`（或 `PUT desired_state {"state":"paused"}`）在「desired=running 且 observed=running 且已收敛、无活动升级编排、无暂停相位、`recovery != fail`」时才接受：一次 generation-fenced 写入 `desired_state=paused`、`pause_phase=capturing`、记录操作员与时间**以及暂停前的恢复指针**，并以既有 `JobCheckpointRecord{kind 按恢复策略}` 通道派发一轮工件（`crates/arkflow-server/src/hub/checkpoint.rs:30`；Job 未声明 checkpoint 配置时跳过该相位直接进入 `stopping`——此时无工件可恢复，resume 等价冷启动，文档明示）。
  2. 工件完成后写入 `pause_phase=stopping` 并复用既有 stop 派发；所有目标节点的观察都变为 stopped 且相位为 `paused` 时，暂停才判定收敛。
  3. `POST /api/v1/jobs/{id}/actions/resume`（或 `PUT desired_state {"state":"running"}`）只在 `pause_phase=paused` 时接受：写入 `desired_state=running`、`pause_phase=resuming`、bump generation，恢复指针保持指向暂停工件；既有 recovery start 路径自动以该工件起跑；观察到 running 收敛后清除暂停注释（`paused_at_ms`/`paused_by`/相位），工件解除 pin。
  4. 失败语义：工件失败/相位 deadline 超时 → 暂停中止，Job 回到 `desired_state=running` 原样运行（旧 generation 未被打断）、**把 `checkpoint_id` 回滚到暂停前记录的指针**（否则失败的 pending/failed 工件会污染后续恢复选择，`hub/checkpoint.rs:49-56`、`placement.rs:1028-1036`），写 `last_error` 与稳定失败码；resume 启动失败 → 走既有 Job 失败语义，工件保持 pin 供重试。
  5. 目标节点离线：stop 派发不到离线节点（`hub/placement.rs:806-829`），暂停保持在 `stopping` 相位并持续 `reconciling`，**不得误判 converged**；节点回归后由后续 tick 继续派发。操作员可随时 `stop` 强制落回 stopped（清除暂停注释与 pin）。
- **崩溃安全**：相位、工件 id 与暂停前指针持久；Hub 重启或 leadership 转移后由协调 tick 幂等重放未完成相位（capturing 重派发一轮工件、stopping 重派发 stop、resuming 重新观察），工件相位有界重试（默认 3 次）与 deadline，耗尽后按第 4 条中止/失败结算。
- **互斥与惰性**：暂停相位非空期间 Job 被协调器围栏（不 start、不重放置、不参与 pressure rebalance）；暂停中拒绝升级（含 atomic 与 stopped 模式），要求先 resume；`stop`/`start`/`restart` 对暂停 Job 有明确定义（`stop` 落回 stopped 并清除暂停注释；`start`/`restart` 等价 resume）。`delete` 释放 pin。
- **保留期**：暂停工件为 `kind="checkpoint"` 时（默认恢复策略），`pause_phase != NULL` 期间 MUST 被 checkpoint 保留清扫跳过——接入既有 pin 过滤（`hub/checkpoint.rs:419-424`），pin 来源新增 `pinned_job_pause_artifacts()`（与编排 pin `hub/job_orchestration.rs:246-258` 并列）；`kind="savepoint"` 时保留期本就不回收（`:409-411`），pin 仅作防御。
- **汇聚判定**：`observe_job` 的 convergence 计算对 `desired_state=paused` 增加判定——只有当 `pause_phase=paused` 且观察为 stopped 时收敛，避免 `paused != stopped` 造成永久 `reconciling`（`hub/lifecycle.rs:27-32`）。Agent 的观测值域保持 `stopped|failed` 不变（`agent/session.rs:417-447`），不新增 Agent 侧 `paused` 观测值，也不新增 Agent 命令。
- **可观测**：SSE 事件 `job.paused` / `job.resumed` / `job.pause_failed`；审计动作 `job.pause` / `job.resume`（接受与拒绝都记，沿用 `record_job_operation_audit` 形状，凭据与 spec 正文不入审计）；Prometheus counter `arkflow_job_pause_total{outcome}`；Job 详情返回暂停字段（additive）。
- **Console**：Jobs 列表/详情增加暂停与恢复按钮、暂停徽标、进行中相位文案、403/冲突降级；en/zh 字典。
- **文档**：Job 生命周期（en/zh）、控制面 API 参考、运维页补充暂停/恢复命令、语义边界（先落工件后停机、工件 kind 与恢复策略、resume 的 replay 窗口、stateless 与 `recovery: fail` Job 的差异）与 CHANGELOG。
- **既有文档纠偏（顺带）**：`docs/reference/api.md` 声称 Job desired-state 支持 `If-Match`/`Idempotency-Key`、且 Job actions「等价于 stream actions」，两处均与实现不符（handler 只读 `current.generation`；`restart` 是 generation bump + 从工件恢复）；在本变更的文档任务中一并修正。

## Non-goals

- **原地热暂停**（内核常驻、只停读源、保留 Kafka 分区分配与热连接）。当前内核只有取消/停止原语（`CancellationToken` + chain 退出），没有 pause gate；实现它需要新的内核原语、新 Agent 命令与心跳/围栏语义，另立专项。v1 的暂停是「持久暂停 = 自动落工件 + 停机」。
- **定时/条件自动恢复**（cron resume、告警恢复后自动继续）、**暂停过期自动 resume**。
- **暂停期间保持连接/租约/分区分配**——暂停期间 Job 不占计算资源，这正是能力价值之一。
- **暂停中升级版本 / 改变并行度**——先 resume，再走既有升级或 rescale 流程。
- **单 task/子任务的单独暂停**——只做 Job 级整体暂停。
- **YAML Stream 的暂停**——Stream 仍是 stop/start，本变更只覆盖分布式 Job。
- **跨 Hub 的暂停协调新协议**——暂停状态存在 `cp_jobs`，随既有存储与选主语义共享；不新增跨实例协议。
- **暂停触发自动 savepoint 之外的额外状态压缩/清理**。

## Capabilities

### New Capabilities

- `job-pause-resume`: Job 级持久暂停/恢复契约——接受前置条件、工件 kind 选择 → stop 的状态机、收敛与失败语义（含指针回滚）、崩溃恢复与幂等、工件保留期保护、暂停互斥（协调/升级/放置）、API 与可观测出口。
- `job-pause-resume` 的工件语义同时经 `checkpoint-recovery`（ADDED：暂停工件在 resume 前可恢复、保留期豁免）与 `control-plane-api`（ADDED：pause/resume 路由、三值 desired-state、稳定错误码与升级互斥）承载。

### Modified Capabilities

- `distributed-job-runtime`（ADDED）：暂停 Job 的收敛由暂停状态机定义，且暂停相位期间不参与协调派发/重放置/再均衡。
- `control-plane-identity`（ADDED）：`job.pause` / `job.resume` 生命周期变更入审计。
- `job-upgrade-orchestration`（ADDED）：暂停相位与升级编排互斥（暂停中拒绝升级；编排中拒绝暂停）。
- `checkpoint-recovery`（ADDED）：暂停工件在 resume 收敛前保持可恢复，checkpoint-kind 工件受保留期豁免。
- `control-plane-api`（ADDED）：pause/resume 路由、desired-state 三值、稳定错误码与升级互斥。
- `control-plane-console`（ADDED）：Console 提供暂停/恢复操作与相位可见性。

## Impact

- `crates/arkflow-server/src/storage/`：`mod.rs`（`JobRecord` 增字段 + 暂停相位读写方法，`SCHEMA_VERSION` v4）、`sqlite.rs`（DDL + ALTER 迁移）、`postgres.rs`/`postgres_methods.rs`（DDL + `ADD COLUMN IF NOT EXISTS` + 方法）；`table_copier::TABLE_ORDER` 不变（列自动随表拷贝），但 row-count/backup/restore 测试需覆盖新列。
- `crates/arkflow-server/src/hub/`：`jobs.rs`（暂停相位写路径）、`placement.rs`（协调 tick 接入暂停状态机 + 围栏）、`checkpoint.rs`（pin 过滤 + savepoint 完成检测）、`lifecycle.rs`（convergence 判定 + stop 观察驱动相位推进）、`observability.rs`（事件/审计）、`error.rs`（稳定失败码）。
- `crates/arkflow-server/src/api/`：`jobs.rs`（pause/resume handler + 互斥校验）、`api_contract.rs`（请求/响应 DTO 增字段）、`mod.rs`（路由）。
- `console/src/`：`features/jobs.tsx`、`api.ts`、`i18n/{en,zh}.ts`。
- `docs/`：`docs/docs/build/jobs.md`、`docs/docs/build/distributed-jobs.md`、`docs/docs/operate/control-plane/{operations,overview,job-upgrades}.md`、`docs/docs/reference/api.md` + zh-Hans 对应树；`CHANGELOG.md`。
- 依赖：零新增（复用既有 savepoint/checkpoint、storage actor、审计与事件漏斗）。
- 兼容性：新字段与相位均为 additive；未被暂停的 Job 行为逐字节不变；Agent 协议与内核零改动（不新增命令，`paused` 永不发给 Agent）；`desired_state` 接受新值属契约扩展。
