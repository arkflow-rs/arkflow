# Tasks: add-hub-postgres-storage

## 1. Backend 抽象

- [x] 1.1 `ControlPlaneBackend` 枚举 + 52 个分派方法 + actor match 臂 await 化
- [x] 1.2 入口 scheme 分派 + 测试构造点更新

## 2. PostgreSQL 实现

- [ ] 2.1 `pg_store.rs`：PG DDL（17 表幂等）+ PgStore 连接/探测
- [ ] 2.2 52 个命令的 sqlx 实现（`$n` 占位、`ON CONFLICT DO NOTHING`、IDENTITY RETURNING）
- [ ] 2.3 live PG 契约套件（覆盖每个方法；无 ARKFLOW_TEST_POSTGRES_URL 时 skip）

## 3. 迁移工具

- [ ] 3.1 `migrate` 子命令（依赖序分块拷贝 + setval + 行数对账）
- [ ] 3.2 迁移测试（live 门控）

## 4. 收尾

- [ ] 4.1 全量验证（test×2 + clippy + docs:check + PG 契约套件 live）
- [ ] 4.2 deploy 文档 en/zh + PLANNING + 归档 + 推送
## 进度注记（多迭代跟踪）

- 2026-09-24：步骤 1 完成并验证——`ControlPlaneBackend`（Sqlite/Postgres）枚举 + 52 个分派方法、actor FIFO await 化、`ARKFLOW_HUB_STORAGE` scheme 分派、全部测试构造点包装。`pg_store.rs` 含幂等 PG DDL（17 表）与 52 个 `Unsupported` 桩。SQLite 全量测试通过（行为不变）。PG 实现按组推进：jobs → nodes/streams → intents/attempts → outbox/events/audit/ops → rollouts/config。本地验证用 PG：`docker run postgres:16 @127.0.0.1:15432`（ARKFLOW_TEST_POSTGRES_URL=postgres://postgres:test@127.0.0.1:15432/arkflow_cp）。
