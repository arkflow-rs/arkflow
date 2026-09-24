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

### 转写器设计（下一步实现用，规则已定稿）

52 个方法的 SQLite 实现已解析到 `/tmp/method_bodies.txt`（`### <name>` 分节）与 `/tmp/method_names.txt`。转写规则（机械映射，生成后编译+live PG 修错）：

1. 签名 `pub fn` → `pub async fn`（`&self` 与参数不变）。
2. `self.with_connection(|connection| { B })` → `{ B' }`（B' 为 B 的 sqlx 转写，执行端 `&self.pool`）。
3. `self.immediate_transaction(|transaction| { B })` → `let mut tx = self.pool.begin().await?; { B' } tx.commit().await?;`（闭包内 `return Err(..)` 语义与方法返回一致，无需改写）。
4. `connection.execute(SQL, rusqlite::params![..])?` → `sqlx::query(SQL').bind(..)...execute(&self.pool).await?;`（事务内执行端 `&mut *tx`）。
5. `connection.query_row(SQL, P, |row| { M })?` → `let row = sqlx::query(SQL')....fetch_one(&self.pool).await?;` + `M'`（`row.get(N)?`→`row.try_get(N)?`）。
6. `query_row(...).optional()?` → `let row = ....fetch_optional(&self.pool).await?;` + `Ok(row.map(|row| M').transpose()?)`（M' 闭包显式标注 `-> Result<T, StorageError>`）。
7. `statement.query_map(P, |row| { M })?; rows.collect()` → `let rows = sqlx::query(SQL')....fetch_all(&self.pool).await?; Ok(rows.into_iter().map(|row| { M' }).collect::<Result<Vec<_>, StorageError>>()?)`。
8. SQL 文本转换：`?N` → `$N`；`INSERT OR IGNORE INTO X (...)` → `INSERT INTO X (...) ON CONFLICT DO NOTHING`（其余语法已验证 PG 兼容）。
9. `transaction.last_insert_rowid()`（仅 record_audit）→ SQL 追加 `RETURNING event_id`，execute 改 `fetch_one` + `row.try_get(0)?`。
10. `execute` 的 usize 返回（prune_*）→ `sqlx::query(...).execute(...).await? as usize`。

注意：`?N` 的 SQL 常量在 `with_connection`/`immediate_transaction` 包装体内——转写器按上述 4/5/6/7 四种调用形态逐个匹配（`connection.prepare` 两步形态先折叠为单步）。

### 实施方式决策（2026-09-24 修订）

放弃正则转写器（方法体含类型化 let、元组行映射、嵌套控制流，规则覆盖不住），改为**逐方法手写**：以 `/tmp/method_bodies.txt` 的 SQLite 实现为蓝本，按组（jobs → nodes/streams → intents/attempts → outbox/events/audit/ops → rollouts/config）手写 52 个 sqlx 方法，每组完成后立即用 live PG（`ARKFLOW_TEST_POSTGRES_URL=postgres://postgres:test@127.0.0.1:15432/arkflow_cp`，容器 arkflow-pg-test 已运行）跑冒烟验证。行映射规则：rusqlite `row.get(N)?` → sqlx `row.try_get(N)?`；`params![..]` → 链式 `.bind(..)`；`?N`→`$N`；`INSERT OR IGNORE`→`ON CONFLICT DO NOTHING`；`last_insert_rowid`（仅 record_audit）→ `RETURNING event_id`。
