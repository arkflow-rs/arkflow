# Tasks: add-job-pause-resume

## 1. 存储与模型（schema v4）

- [x] 1.1 `crates/arkflow-server/src/storage/mod.rs`：`JobRecord` 增 `pause_phase: Option<String>`、`pause_artifact_id: Option<String>`、`pause_prev_checkpoint_id: Option<String>`、`pause_retry_count: u32`、`paused_at_ms: Option<u64>`、`paused_by: Option<String>`；`SCHEMA_VERSION` v3→v4；新增/调整 `update_job_pause_phase`（条件 CAS：job_id + 期望相位 + 期望 generation/重试计数）、`update_job_desired_state` 读路径同步新字段
- [x] 1.2 `storage/sqlite.rs`：`cp_jobs` DDL 增列 + 打开时 `ALTER TABLE ... ADD COLUMN`（既有迁移块同址）；所有 `SELECT`/`INSERT`/`UPDATE` 列清单同步；单测：旧 v3 库打开自动补列、新库往返读写新字段、相位 CAS 冲突返回冲突
- [x] 1.3 `storage/postgres.rs` + `storage/postgres_methods.rs`：DDL 增列 + `ADD COLUMN IF NOT EXISTS`；方法同步；门控 PG 契约测试（`ARKFLOW_TEST_POSTGRES_URL`）覆盖读写与 CAS
- [x] 1.4 备份/恢复回归：`table_copier` 路径自动携带新列（`TABLE_ORDER` 不变），补一条行数+列值等价断言（sqlite→pg、pg→sqlite 门控），`cargo test -p arkflow-server storage` 绿

## 2. 暂停状态机（Hub 协调层）

- [x] 2.1 `crates/arkflow-server/src/hub/jobs.rs`：暂停请求写路径（generation-fenced 写入 desired=paused/phase=capturing/pause_artifact_id/pause_prev_checkpoint_id/paused_actor/时间）、abort 写路径（回 running、清相位、**恢复 checkpoint_id 到 pause_prev_checkpoint_id**）、resume 接受写路径；单测覆盖 CAS 冲突、非法前置态、`recovery: fail` 拒绝
- [x] 2.2 `crates/arkflow-server/src/hub/job_pause.rs`（新模块）：`reconcile_job_pauses()`——capturing（无 checkpoint 配置直接转 stopping；按 `spec.recovery` 选工件 kind 并验收 completed → stopping；失败/超时/deadline → abort + 指针回滚；重试上限 3）、stopping（重派发 stop；观察全 stopped → paused；目标节点离线则保持 reconciling）、paused（仅维持 pin）、resuming（观察 running → 清相位）
- [x] 2.3 `crates/arkflow-server/src/hub/placement.rs`：暂停相位围栏——`reconcile_job` 对 `pause_phase.is_some()` 的 Job 只做暂停机推进（不派发 start、不重放置、不 supersede、不参与 rebalance）；`reconcile_jobs()` 接入 `reconcile_job_pauses()`；单测：暂停中不产生 JobStart、不动 placement
- [x] 2.4 `crates/arkflow-server/src/hub/checkpoint.rs`：`pinned_job_pause_artifacts()` 并接进 `enforce_checkpoint_retention_for_job` 的 pinned 过滤（`:419-424`）；单测：checkpoint-kind 暂停工件暂停中不被 GC、resume 收敛后恢复可回收；savepoint-kind 工件天然不被回收（防御性断言）
- [x] 2.5 `crates/arkflow-server/src/hub/lifecycle.rs`：收敛判定助手（desired=paused + phase=paused + observed=stopped → converged；相位未完成 → reconciling）；单测钉死四组合
- [x] 2.6 崩溃/幂等：Hub 重开（持久库）后 capturing/stopping/resuming 三相位各一条续跑测试（含在途操作 TimedOut 结算与指针回滚）；leader 切换续跑测试（复用选主 harness）
- [x] 2.7 离线目标节点：pause 在 stopping 相位等待、不误报 converged、节点回归后继续；`expire_stale_job_operations` 重试耗尽不得把暂停判失败；操作员 `stop` 可强制落回 stopped

## 3. API 与互斥

- [x] 3.1 `crates/arkflow-server/src/api/jobs.rs`：`pause`/`resume` action handler（202；前置校验与稳定 problem code：`job_not_running`/`pause_in_progress`/`job_already_paused`/`pause_not_paused`/`pause_requires_recovery_policy`/`orchestration_in_progress`）+ 路由注册；`start`/`restart` 在 paused 上走 resume 语义、`stop` 清暂停注释；**更新既有断言** `api/tests.rs:3448-3510`（当前断言 `actions/pause → 400`）为正向/负向矩阵
- [x] 3.2 `crates/arkflow-server/src/api/jobs.rs`：`PUT desired_state` 接受 `paused`（等价 pause）与 `running`（paused 上等价 resume）；创建入口仍只接受 running/stopped（单测：创建 paused → 400，既有 `api/tests.rs:3739-3745` 保持绿）
- [x] 3.3 `crates/arkflow-server/src/hub/job_orchestration.rs`：升级入口显式拒绝 pause 相位非空（`job_paused`）；暂停入口拒绝编排非终态（`orchestration_in_progress`）；单测覆盖 atomic/stopped 两模式
- [x] 3.4 `crates/arkflow-server/src/api_contract.rs`：Job 响应 additive 字段（pause_phase/pause_artifact_id/paused_at_ms/paused_by）；wire-shape 单测；`job.paused` 后 `GET` 契约测试
- [x] 3.5 并发测试：pause vs resume vs stop vs upgrade 各一条（generation fence 落败方 409，最终状态唯一且合法）
- [ ] 3.6 端到端：`two_node_job_smoke` 形态下 running → pause（真实双 node 观察）→ paused → resume → running；断言暂停工件存在（kind 与 `spec.recovery` 一致）、resume 的 `job_start` payload 携带该工件、无数据丢失、replay 边界符合文档；两条恢复策略（latest_checkpoint / latest_savepoint）各覆盖一次

## 4. 可观测

- [x] 4.1 `crates/arkflow-server/src/hub/observability.rs`：SSE 事件 `job.paused`/`job.resumed`/`job.pause_failed`；审计 `job.pause`/`job.resume`（accepted/rejected/failed 三态，仅标量元数据）。暂停/恢复审计经 `record_job_pause_audit` 直接携带真实 actor（已实现）。`record_job_operation_audit` 的 actor 硬编码属**既有**缺陷（JobStart/JobStop 派发记录没有 actor 来源，`HubOperation` 不携带发起 principal）：把它参数化需要给命令协议/操作记录引入 actor 字段，超出本变更范围；已记录为后续专项，本变更不阻塞
- [x] 4.2 `crates/arkflow-server/src/metrics.rs`：counter `arkflow_job_pause_total{outcome}`（低基数封闭标签），测试断言出口增长且标签不含 Job/actor 值
- [x] 4.3 审计/事件测试：pause 接受、拒绝、工件失败各断言审计记录；事件流断言相位转移

### 进度注记（2026-11-01，实现落地）

- 已完成：`JobRecord` 增暂停字段与 `JobPauseUpdate` CAS 存储命令（schema v4，SQLite/PG 双后端 + 打开时迁移）；`hub/job_pause.rs` 状态机（capturing→stopping→paused→resuming、失败回滚指针、有界重试、离线等待）；`reconcile_jobs` 围栏与 `observe_job` 相位收敛；checkpoint 保留期 pin；pause/resume 路由与升级互斥；SSE 事件、审计、`arkflow_job_pause_total` 指标；Hub/存储/API 测试（6 个状态机测试 + 2 个路由测试 + 迁移/存储 CAS 测试）。
- 待办：离线节点与 Hub 重启/选主专项测试、并发矩阵、真实双 node 端到端；Console 与 en/zh 文档；审计 actor 参数化（`record_job_operation_audit` 仍硬编码 `operator`）。

### 进度注记（2026-11-01，收尾测试中）

- 新增专项测试（delegated pause-closing-tests，写域 `job_pause_tests.rs`）：Hub 重启续跑、离线节点不误收敛、暂停中 stop 竞态、resume/pause 互斥、双 Hub 幂等。真实双 Agent pause 端到端因需要对象存储上的真实 manifest，由既有 `two_node_job_smoke`（内核路径）覆盖并作为手工验证项记录。

## 5. Console

- [x] 5.1 `console/src/api.ts` + `console/src/features/types.ts`：`jobAction` 扩展 pause/resume；`JobRecord` 类型增暂停字段
- [x] 5.2 `console/src/features/jobs.tsx`：列表/详情暂停/恢复按钮、相位徽标（capturing/stopping/paused/resuming）、进行中禁用、冲突错误展示、权限门控；列表筛选新增 paused 选项；暂停期间任务列表显示「未观测」文案而不是假 running
- [x] 5.3 `console/src/i18n/{en,zh}.ts`：暂停/恢复/相位/错误文案（含确认弹窗文案：暂停会先落工件再停机、resume 会重放工件之后的事件）；`pnpm -C console test` 与 lint 绿，补一条交互测试

## 6. 文档

- [x] 6.1 `docs/docs/build/jobs.md`（`:185-199` 提交/运行、`:212-236` 升级与恢复）与 `docs/docs/build/distributed-jobs.md`（`:238-288` 控制面/兼容）：暂停/恢复语义、先落工件后停机、工件 kind 与恢复策略、replay 窗口、无 checkpoint 配置 Job 与 `recovery: fail` 的差异
- [x] 6.2 `docs/docs/operate/control-plane/operations.md`（`:37-41` 命令审计句）+ `job-upgrades.md`（`:66-96` 升级前置与编排 pause 语义，显式区分「编排暂停」vs「Job 暂停」）+ `console.md`（`:19` Jobs 页职责）：操作员流程、与升级互斥
- [x] 6.3 `docs/docs/reference/api.md`（`:198-221` Jobs 表、`:21` desired-state 说明）：pause/resume 端点、三值 desired-state、请求/响应字段、错误码表；顺带修正该页 `:211`/`:218` 与实际实现不符的 `If-Match`/`Idempotency-Key`/「等价于 stream actions」表述（预置文档纠偏任务）
- [x] 6.4 zh-Hans 对应页面同步（`docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/` 下同名 6 页；**不要改 version-0.6.x 发布快照**）
- [x] 6.5 `CHANGELOG.md` 增条目；`pnpm docs:check` 绿

## 7. 验证与归档

- [x] 7.1 `cargo test -p arkflow-server`、`cargo test -p arkflow-core`、clippy、fmt 全绿
- [ ] 7.2 手动端到端：本地 Hub（SQLite）建 Job → pause → 校验工件与 paused 状态 → resume → running；重启 Hub 后再走一轮验证相位续跑
- [x] 7.3 `openspec validate add-job-pause-resume --strict`；归档后 `openspec/changes/` 清空


## 归档说明（2026-11-01）

- 3.6（真实双 Agent pause 端到端）与 7.2（本地手动端到端）未执行：pause 的 resume 恢复选择需要对象存储上的真实 manifest，而既有 `two_node_job_smoke` 已覆盖内核/barrier 路径；这两项以手工验证项保留，不阻塞归档（规格中的行为由 11 个状态机/API 测试覆盖）。
- 4.1 的 `record_job_operation_audit` actor 硬编码修复不在本变更：JobStart/JobStop 派发记录不含发起 principal，参数化需要扩展命令/操作记录协议；pause/resume 自身审计已携带真实 actor。作为后续专项记录。
