# Design: add-hub-backup-restore

## Context

存储层 20 张 `cp_*` 表、schema version 3（`storage/mod.rs:527`）；storage actor 串行化所有访问（`StorageCommand` 枚举 + 宏分发，写路径包 `Fenced` 围栏）。`migrate_tool.rs` 已有：`TABLE_ORDER`（18 表，漏 `cp_webhooks`）、列发现、分块拷贝（1000 行/事务）、自引用表父行优先、行数校验、PG 序列重置。`migrate` 子命令走 `bin/arkflow-server.rs` → `bootstrap::run_migrate` 分发先例。SQLite `VACUUM INTO` 产出单文件一致快照（含 schema 与 `user_version`）。

## Goals / Non-Goals

**Goals:**
- 单命令产生一致快照、单命令恢复到空库；两种源后端、两种目标后端全组合可用。
- 恢复 fail-closed：非空目标、版本不兼容、行数不符都显式失败。
- 修复 migrate 漏拷 `cp_webhooks` 的存量缺口。

**Non-Goals:** 见 proposal（调度、合并恢复、PITR、Console UI、加密、在线恢复）。

## Decisions

- **D1 统一快照格式 = SQLite 数据库文件**（业务表 + schema 版本，不含 `cp_hub_lease`）：恢复侧只需一个读取器；跨后端恢复免转换。PG 源不走 `pg_dump`——外部二进制依赖会让「工具开箱即用」变成环境命题，且仓库既有路线是进程内表拷贝（migrate 先例）。
- **D2 SQLite 源备份走 actor 命令 + `VACUUM INTO`**：actor 串行化天然提供无并发写窗口；`VACUUM INTO` 是 SQLite 原生一致快照（持读锁、含 WAL 已合并内容），比逐表拷贝快且零丢失面。快照后处理 `DELETE FROM cp_hub_lease` 使两条源路径产物一致（租约行永不进快照）。actor 命令为**非围栏读**——读不需要 epoch 围栏，照既有读命令的通路。
- **D3 PG 源备份 = 单 `REPEATABLE READ` 事务内逐表读取**：20 表跨表一致性靠快照事务（migrate 不需要是因为源是离线文件）；写入端复用 SQLite 插入（新增 `sqlite_insert_chunk`，与 `pg_insert_chunk` 对偶），目标文件由全新 `SqliteBackend::open` 创建（已存在即 fail-closed）。逐表行数校验（源事务内 count == 目标 count）+ 总数复核。
- **D4 restore = 打开快照（经既有版本校验与迁移路径）→ 校验目标空 → 复制 → 校验 → 序列重置**：
  - 目标空校验：对每张 `TABLE_ORDER` 表 `SELECT EXISTS(SELECT 1 FROM ...)`，任一非空中止并列出表名（一张一张查，给出全部非空表而不是首个即停，便于诊断）；
  - 快照版本新于二进制 `SCHEMA_VERSION` 拒绝（复用既有 `user_version` 检查语义；PG 目标侧同理比对后端 schema 版本）；
  - 序列重置：PG 走既有 `setval` 循环；SQLite 目标 `sqlite_sequence` 随行插入自动推进，无需处理（AUTOINCREMENT 语义）；
  - 恢复期间目标库由 restore 进程独占打开——Hub 同时运行会因 SQLite 写锁/PG 连接冲突失败，错误信息指导「停止 Hub 后重试」；
  - **实施强化（CodeRabbit 评审后）**：快照以**临时工作副本**打开（`SqliteBackend::open` 会翻转 journal mode，归档快照必须保持字节不变，且只读介质可恢复）；PG 侧整个恢复（`ACCESS EXCLUSIVE NOWAIT` 逐表锁 → 空库校验 → 拷贝 → 行数校验 → `setval`）在**单个事务**内执行——有活动事务的会话使恢复立刻以 `TargetBusy` 失败，任何失败整体回滚，重试总是面对干净空库；`migrate` 保留逐 chunk 提交语义不变。**诚实边界（第二轮 CR 修正措辞）**：`NOWAIT` 锁只能检出此刻正持有锁的会话——空闲的 Hub（事务间隙）不持锁、不被检出，SQLite 写锁同理；「先停全部 Hub」仍是运维责任，恢复只保证「活动写者被拒 + 期间无写入者能插入 + 全有或全无」。
- **D5 `TABLE_ORDER` 增补 `cp_webhooks`**：webhook 订阅无外键依赖，追加在 `cp_events` 之后的任意稳定位置；migrate 与 backup/restore 共用，`migrate` 的 sqlite→pg 从此不再丢订阅（行为修复，附迁移前后的行数校验天然覆盖）。
- **D6 CLI**：`backup --from <store> --to <file>` / `restore --from <file> --to <store>`，与 `migrate` 相同的 `bootstrap::run_*` 形态（可测试、错误经 sink 输出）；退出码 0/非 0。restore 前打印将被覆盖的数据库 scheme 与目标（无 `--force`，非空即拒）。实施澄清：store 参数沿用 `ARKFLOW_HUB_STORAGE` 的常规 scheme 分派（`postgres://` 前缀→PG，其余为 SQLite 路径），与 `migrate` 的 `--from/--to` 旗标结构同构——显式、可测试、不耦合环境变量。另加 SQLite 目标守卫：已存在的非空文件若完全不含 `cp%` 表（即不是控制面库）直接拒绝，避免把无关 SQLite 文件 DDL 成控制面库。

- **D7（实施补充）SQLite 源备份的计数窗口**：源行数统计与 `VACUUM INTO` 放进同一次连接锁窗口（actor 串行化保证进程内无交错）。跨进程备份运行中的 Hub 时，窗口内落进来的写会使行数校验失败——fail-closed 并清除半成品文件，文档写明「写入静止后重试或停 Hub」。快照输出统一为非 WAL 单文件（`VACUUM INTO` 产物天然非 WAL；PG 源路径写完后 `journal_mode=DELETE` 折叠 WAL）。

## Risks / Trade-offs

- **PG 源备份占用读连接**：单事务长读在 PG 上持 MVCC 快照，大库备份期间膨胀 `pg_stat_activity` 快照_window——cp_* 表有界（retention 双界清扫），实际量级小；文档提示低峰执行。
- **快照格式与 schema 版本绑定**：旧版本快照依赖打开时迁移路径升级——既有机制已覆盖（schema version 测试先例）；新于二进制的快照拒绝，文档写明「升级二进制后再恢复」。
- **`cp_hub_lease` 不恢复 → epoch 归零重启**：围栏是库内 CAS 语义，恢复库就是新事实源；与 HA 部署的组合（恢复到共享 PG 的备库场景）在文档标注「恢复前停全部 Hub 实例」。
- **VACUUM INTO 的瞬时磁盘翻倍**：快照写往 `--to` 指定路径（可在另一文件系统），文档提示容量。
