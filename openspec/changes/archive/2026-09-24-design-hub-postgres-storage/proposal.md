## Why

Hub HA 路线（openspec/specs/hub-ha/architecture.md）的阶段 1：Hub 存储从 SQLite 迁移到 PostgreSQL。本变更为纯设计（不改代码），在动手前把 backend 抽象、schema 映射、迁移工具与测试策略定稿，作为实施 change 的蓝图。

## What Changes

- 产出评审级设计文档 `openspec/specs/hub-ha/postgres-storage-design.md`：
  1. **Backend 抽象**：`StorageBackend` async trait（async_trait），SQLite（rusqlite 同步，现路径）与 PostgreSQL（sqlx PgPool）双实现；StorageActor 的 tokio 任务保留 FIFO 串行处理，顺序不变性不破坏——async 化仅是把 match 臂内的同步调用换成 trait 调用。
  2. **Schema 映射**：17 张 `cp_*` 表的 SQLite→PG 类型映射表（INTEGER→BIGINT、BLOB→BYTEA、epoch-ms 保持 INTEGER 语义）、索引与约束逐表对照；DDL 以启动时执行的迁移脚本管理（与 SQLite 的 `CREATE TABLE IF NOT EXISTS` 策略对齐）。
  3. **配置面**：`ARKFLOW_HUB_STORAGE` 按 scheme 分派——`postgres://` 走 PG，裸路径保持 SQLite（零破坏向后兼容）。
  4. **迁移工具**：`arkflow-server migrate --from sqlite:<path> --to postgres:<url>` 一次性拷贝（表级顺序、事务分块、行数校验）。
  5. **事务映射**：`BEGIN IMMEDIATE`→PG 默认读写事务；唯一约束冲突语义对照；连接池参数（max_connections、acquire_timeout）。
  6. **测试策略**：现有 SQLite 契约测试保持；PG 契约套件按仓库惯例 `#[ignore]` + `ARKFLOW_TEST_POSTGRES_URL` 门控（live），SQL 文本断言离线覆盖。
  7. **风险与决策记录**：方言差异、PG 不可用时 Hub 启动快败、outbox 顺序、行为保持清单。

## Capabilities

### Modified Capabilities

- `hub-ha`: 新增 PostgreSQL 存储后端设计需求（阶段 1 蓝图）。

## Impact

- 仅文档：`openspec/specs/hub-ha/postgres-storage-design.md` + 本 change 的 delta/tasks。
- 后续实施 change：`add-hub-postgres-storage`（本设计定稿后立项）。

## Non-goals

- 不实现任何代码；不做 leader election（阶段 2）；不做 Agent 多 Hub 发现（阶段 2 后续）。
- 不决定 PG 部署形态（流复制/云 RDS 属运维文档）。
