# Proposal: add-hub-backup-restore（控制面 DB 备份/恢复）

## Why

控制面全部状态（节点目录、Job/版本/任务、intents/attempts、配置版本、审批、rollout、审计、事件、webhook）持久化在单一后端数据库（20 张 `cp_*` 表，`storage/sqlite.rs` / `storage/postgres.rs`，schema version 3，`storage/mod.rs:527`），但**没有任何快照/导出/恢复工具**——磁盘损坏、误删、迁移失败后唯一恢复手段是重放外部事实。既有 `migrate` 子命令（`bin/arkflow-server.rs:9`，`storage/migrate_tool.rs`）只做一次性 sqlite→pg 搬迁，不做备份。PLANNING 11.2-4 与 11.4 将其列为 P1「企业入场券」；Job 级 checkpoint/savepoint 恢复是数据面语义，与本变更（控制面元数据）互补、勿混淆。

## What Changes

- **`arkflow-server backup --to <file>` 子命令**：产生**统一 SQLite 格式快照**（目标文件必须不存在，fail-closed），覆盖 `TABLE_ORDER` 全部业务表（含既有 `migrate` 工具遗漏的 `cp_webhooks`，见下）：
  - SQLite 源：经 storage actor 发非围栏读命令执行 `VACUUM INTO`——actor 串行化保证快照窗口无并发写，文件级一致且含 schema 版本；随后在快照文件内 `DELETE FROM cp_hub_lease`（租约行不进快照，见语义安全）；
  - PostgreSQL 源：打开一个 `REPEATABLE READ` 事务保证跨表读一致性，按 `TABLE_ORDER` 逐表读取写入新建的 SQLite 快照文件（复用 migrate 工具的列发现/读表机制），逐表行数校验；快照写入源库当时的 `SCHEMA_VERSION`；不拷贝 `cp_hub_lease`。
- **`arkflow-server restore --from <file>` 子命令**：把快照导入**空**目标库（经常规 scheme 分派选定后端）：
  - **空库 fail-closed**：任一 `cp_*` 业务表非空即中止并指明表名——v1 不做合并恢复、无 `--force`；
  - 快照 schema 版本按既有兼容规则校验（新于二进制拒绝；旧于二进制经打开时既有迁移路径升级后再拷贝）；
  - 拷贝 `TABLE_ORDER`（自引用表父行优先，复用 migrate 机制）+ 逐表行数校验 + 序列/`sqlite_sequence` 重置；
  - 恢复目标可以是与快照不同的后端种类（快照恒为 SQLite 格式，跨后端恢复天然成立）。
- **修复既有缺口**：`migrate` 工具的 `TABLE_ORDER` 漏了 `cp_webhooks`（webhook 表 #1338 落地时未更新拷贝清单，sqlite→pg 迁移会静默丢全部订阅）——本变更把 `cp_webhooks` 纳入 `TABLE_ORDER`，迁移与备份共用同一清单。
- **CLI 形态**：与 `migrate` 子命令同构的参数分发（`bin/arkflow-server.rs` → `bootstrap::run_backup` / `run_restore`），`--help` 文案；Hub 运行中执行 restore 直接拒绝（目标库被打开即失败，无需探测进程）。
- **诚实边界**：快照文件含配置版本 payload 原文（`secret:`/`env:`/`file:` 引用保持未解析形态，但 operator 写入配置的**字面**凭据会原样进入快照）——文档明示按数据库同等密级保管。
- **语义安全**：`cp_hub_lease` 不进快照（两条备份路径一致排除）——恢复后新 Hub 经既有 CAS 重建租约行并推进 epoch，围栏是库内语义，与灾难前 epoch 单调性解耦；未处理 `cp_outbox` 行恢复后由既有 reconciliation 重放收敛，节点重新注册即可。

## Non-goals

- **定时/自动备份、备份轮换与保留策略**——调度是运维层（cron/K8s CronJob）的事，工具只负责单次一致性快照。
- **合并恢复 / 部分恢复（单表、按时间点）**——v1 只做空库全量恢复；PITR 需要 WAL 归档，另行评估。
- **Console 备份管理 UI / 备份下载 API**——CLI 先行；HTTP 面暴露快照下载涉及大响应与鉴权面，另行立项。
- **快照加密**——文件密级靠文件系统权限与运维流程。
- **数据面 checkpoint/savepoint 的备份**——那是 Job 状态存储（redb/S3），独立信任域。
- **在线 restore**——恢复目标库必须停机操作。

## Capabilities

### New Capabilities

- `hub-backup-restore`: 控制面数据库备份/恢复契约——统一 SQLite 快照格式、两种源后端的一致性语义、空库恢复 fail-closed、schema 版本兼容、行数校验与序列重置、CLI 子命令行为。

## Impact

- `crates/arkflow-server/src/`：`storage/`（actor 增 backup 命令；`migrate_tool.rs` 泛化为双向 copier 或并列模块；PG 读取端）、`bootstrap.rs`（`run_backup`/`run_restore`）、`bin/arkflow-server.rs`（参数分发）。
- 依赖：零新增。
- 兼容性：纯新增子命令，无既有行为变化；快照文件格式随 schema version 演进（由版本校验与既有迁移路径承载）。
- 文档：`docs/docs/operate/control-plane/deploy.md` 备份/恢复节（en/zh）、CHANGELOG。
