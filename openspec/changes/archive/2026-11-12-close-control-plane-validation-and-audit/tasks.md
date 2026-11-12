## 1. 审计身份

- [x] 1.1 将真实 actor 从 Job 请求传到派发审计，后台操作使用 system；添加接受/拒绝与凭证脱敏回归。
- [x] 1.2 操作 actor 通过既有 JSON 持久化、恢复和重试保留；添加历史 JSON 兼容与恢复/重试测试。

## 2. 审计读取

- [x] 2.1 审计路由强制 ReadAudit 与 audit scope；添加 admin/viewer/operator/未认证/越权矩阵测试。
- [x] 2.2 增加 actor/action/from_ms/to_ms 过滤，验证先过滤后分页、时间端点及非法范围。

## 3. 真实暂停恢复

- [x] 3.1 真实双 Agent + 内核端到端覆盖 latest_checkpoint 与 latest_savepoint 的 pause/resume、工件、恢复命令和恢复后数据推进。
- [x] 3.2 持久 SQLite Hub 重启后再次暂停/恢复，验证暂停状态与真实恢复路径；运行对应集成测试。

## 4. PostgreSQL HA 身份一致性

- [x] 4.1 增加真实共享 PostgreSQL 下两个 Hub 选主、凭证吊销后跨 leader 401 及 stale epoch 身份写拒绝测试。
- [x] 4.2 准备隔离 PostgreSQL 环境并实际运行门控测试，记录结果并清理测试资源。

## 5. 文档与最终验证

- [x] 5.1 同步英文与 zh-Hans API/审计运营文档、CHANGELOG，说明审计权限收紧和过滤参数。
- [x] 5.2 依据实际结果更新旧归档任务及 PLANNING，消除已完成条目和过时的归档尾项说明。
- [x] 5.3 运行 server/core 相关测试、fmt/clippy、docs:check、OpenSpec strict validate；核对 diff 与任务证据。
- [x] 5.4 校验实现符合规格，完成后同步主规格并归档本收尾 change。

## 验证记录（2026-11-12）

- `cargo test -p arkflow-server --lib hub::operations::job_actor_tests`：6 项通过。
- 真实 PostgreSQL 16：`postgres_identity_revocation_survives_failover_and_fences_stale_writes` 实跑通过（非门控跳过），隔离 schema 已删除；`api::audit_api_tests` 4 项通过。SQLite 下同组审计测试亦通过。
- `cargo test -p arkflow-server --lib --bins`：526 项通过；其余 6 组集成套件（backup_restore/bootstrap/data_plane_metrics/fleet_readiness/kernel_job_lifecycle/node_resource_reporting）：19 项通过、2 项忽略。
- `cargo test -p arkflow-core --lib`：1143 项通过、2 项忽略。
- `cargo fmt --all -- --check`、`git diff --check`、`cargo clippy --workspace --all-targets` 通过，Clippy 无告警。
- `pnpm -C docs docs:check`：145 页、50 个组件、2 份 README 检查通过；本 change 的 OpenSpec strict validate 通过。
- 使用完整 operator/node 认证和持久 SQLite 的真实 API 预览验证：未认证 `/audit` 返回 401，合法组合过滤返回 200，倒置时间范围返回 400 `invalid_audit_query`。预览服务、临时配置与 SQLite 文件已清理。
- 双 Agent 实跑通过：`two_node_pause_resume_latest_checkpoint_survives_sqlite_hub_restart` 与 `..._latest_savepoint_...` 各 2 轮 capture，首轮 paused 时重启 SQLite Hub，第二轮在重启后完成；真实 manifest、state snapshot、恢复指针、`job_start` payload、逐条 sequence 与 state_count、两 source 最终 24 条均通过。`hub::job_pause_tests` 12 项同时通过。
- 实跑发现并修复两处生产缺陷（`hub/job_pause.rs`）：① capture 在途判定只覆盖 checkpoint/savepoint，未含 commit，协调器会在 commit 窗口内误判无在途并替换工件，导致重试耗尽后 `pause_savepoint_failed`；② 相位写入一律置 `reconciling`，但 resume 清除路径缺少收敛校正，恢复成功后 Job 永久停在 `reconciling`。两处均由新 E2E 确定性复现，修复后两种恢复策略实跑通过。
- PostgreSQL 测试资源已清理：隔离 schema 由测试自行删除；临时容器 `arkflow-closure-postgres` 经用户确认后停止（`--rm`，已移除）。
