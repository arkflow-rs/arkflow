## 1. 核实与契约基线

- [x] 1.1 核实 `hub.rs:2540` `command.dispatch` 审计 message 的实际内容:若含未脱敏配置/凭证,按 design D2 收敛;无论结论,补一条"审计记录不含凭证与 Job 配置体"的单元测试钉住(identity spec MUST NOT 约束)。
- [x] 1.2 盘点 `crates/arkflow-server/src/hub.rs` 全部 Job mutation 入口(start/stop/checkpoint/恢复/放置决策)与现有拒绝分支(fencing、Capacity、鉴权),形成审计埋点清单(后续任务逐一覆盖)。

## 2. Job 操作审计

- [x] 2.1 在 Job mutation 入口的接受/拒绝分支调用 `record_audit_event`:覆盖真实操作词表 `job_start`/`job_stop`/`job_checkpoint`/`job_savepoint`,审计 action 映射为 `job.start`/`job.stop`/`job.checkpoint`/`job.savepoint`(design D1),resource_type=`job`,message 仅含标量元数据(generation、operation、目标节点、failure_code);恢复随 start 记录携带,`*_commit` 走既有 `command_result` 路径不新增审计点。
- [x] 2.2 审计查询路径验证:`GET /api/v1/audit?resource_id=<job_id>` 能取回 Job 生命周期记录;如需索引,补 `(resource_type, resource_id)` 索引。
- [x] 2.3 回归测试:Job start 被接受 → 审计落库(outcome=accepted);Job stop 被 fencing/Capacity 拒绝 → 审计落库且 message 不含配置体;拒绝请求(401/403)不留 actor 误导记录(按 identity spec 既有场景)。

## 3. 命令延迟与失败率指标

- [x] 3.1 在 Hub 增加 per-command-type 原子累积器:固定命令类型枚举(design D4)、延迟分桶(5/10/25/50/100/250/500/1000/2500/5000/10000/+Inf ms)、成败计数(outcome_class 固定枚举);在操作迁移到 acknowledged/terminal 处更新(enqueue→ack 取 `created_at_ms` 差值)。
- [x] 3.2 `hub_metrics`(`lib.rs:2270`)渲染 `arkflow_command_duration_bucket{command=...,le=...}` 与 `arkflow_command_total{command=...,outcome=...}`;确认重启后归零是显式文档化行为(spec 场景表述)。
- [x] 3.3 回归测试:ack 后 scrape 出对应 command 的延迟桶;`node_unavailable` 失败递增对应 outcome_class 计数;大量 Job/节点不产生新标签(series 有界)。

## 4. Job 操作过期与重试上限

- [x] 4.1 `cp_operations` 迁移:PRAGMA 版本 +1,`ALTER TABLE` 追加 `expires_at_ms INTEGER`、`retry_count INTEGER NOT NULL DEFAULT 0`(沿用 `storage.rs` 既有迁移模式);旧库自动补列,旧行无过期语义。
- [x] 4.2 周期 sweep 增加分支:非终态且 `expires_at_ms < now` → 置可重试并 `retry_count += 1`;达到上限(新配置项,默认 3)→ 终态 `failed`(failure_class=`expired`);重入队走既有 reconciliation;不删除操作记录(dedup 记录保留)。
- [x] 4.3 回归测试:过期 queued 操作被置可重试且计数递增;达上限后终态不再入队;无 `expires_at_ms` 的旧记录行为不变;过期操作仍充当 (job_id, generation, operation) 去重记录。

## 5. 审计保留(cleanup)

- [x] 5.1 周期 sweep 增加 `cp_audit_events` 清理:超过保留时间(默认 30 天)且超出条数上限(默认 100k)的旧行删除,与现有 `cp_operations`/`cp_job_checkpoints` 清理同一节拍;保留策略可配置。
- [x] 5.2 回归测试:超界审计行被回收;保留界内最近记录仍可查询;`GET /api/v1/audit` 行为不变。

## 6. 收口

- [x] 6.1 若可行,在 `api_contract.rs` 契约测试处补"每个 mutating Job 路由触发审计写入"的防回归断言;不可行则在 design.md 的 Risks 记录原因。
- [x] 6.2 `cargo test -p arkflow-server` 全绿;`cargo clippy --workspace --all-targets` 无新增告警。
- [x] 6.3 更新 `docs/docs/control-plane/http-api-v1.md` 与 `docs/docs/control-plane/`(如存在 observability/audit 页)说明新指标名、审计 action 枚举与保留默认值;核对 `pnpm docs:check`。
