# Tasks: add-hub-backup-restore

## 1. 表拷贝层泛化（storage/）

- [x] 1.1 `migrate_tool.rs` 的 `TABLE_ORDER` 增补 `cp_webhooks`；既有 sqlite→pg 迁移测试覆盖订阅保留（行数断言）
- [x] 1.2 抽出可复用 copier：源读取（sqlite 列发现/读表 + 新增 PG 事务内读表）、目标写入（既有 pg_insert_chunk + 新增 `sqlite_insert_chunk`）、父行优先、行数校验、序列重置（PG `setval`）——保持 `migrate_sqlite_to_postgres` 行为不变（既有测试全绿）
- [x] 1.3 单测：copier 对 (sqlite→sqlite)、(pg→sqlite) 组合的往返等价（门控 PG 测试用 `ARKFLOW_TEST_POSTGRES_URL`）

## 2. backup 子命令

- [x] 2.1 storage actor 增非围栏备份命令（SQLite 路径执行 `VACUUM INTO` 到指定文件）+ 快照内 `DELETE FROM cp_hub_lease` 后处理
- [x] 2.2 `bootstrap::run_backup`：scheme 分派——SQLite 源走 2.1；PG 源走 D3（REPEATABLE READ 单事务读取 → 新建 SQLite 快照 + 行数校验）；目标文件已存在 fail-closed
- [x] 2.3 集成测试：活 SQLite Hub（并发写进行中）快照行数一致、无 lease 行；PG 源快照等价（门控）；`--to` 已存在拒绝

## 3. restore 子命令

- [x] 3.1 `bootstrap::run_restore`：打开快照（既有版本校验/迁移路径）→ 空库校验（列全部非空表再中止）→ `TABLE_ORDER` 拷贝（pg/sqlite 双目标）→ 行数校验 → PG `setval` 重置；错误经 sink 输出、非零退出
- [x] 3.2 集成测试：快照→空 SQLite、快照→空 PG（门控）全表等价；非空目标拒绝并列出全部非空表；快照版本新于二进制拒绝；恢复后 Hub 可启动且 reconcile 正常（复用 two-node smoke 形态或最小 reconcile 测试）

## 4. CLI 与验证

- [x] 4.1 `bin/arkflow-server.rs` 分发 `backup`/`restore` 参数（与 `migrate` 同构）+ `--help`；`cargo test -p arkflow-server` 全绿 + clippy/fmt 干净
- [ ] 4.2 真实运行验证：本地起 SQLite Hub 写入数据 → backup → 新库 restore → 起Hub → 节点注册/配置版本/审计可见；migrate 漏 webhooks 修复实测（含订阅的库迁移后订阅仍在）

## 5. 文档

- [x] 5.1 `docs/docs/operate/control-plane/deploy.md` 增备份/恢复节：命令、一致性语义、cp_hub_lease 不进快照、停机要求、密级提示（en）
- [x] 5.2 zh-Hans 对应树同步
- [x] 5.3 `pnpm docs:check` 绿；CHANGELOG 增条目（含 migrate 漏 cp_webhooks 的修复说明）
