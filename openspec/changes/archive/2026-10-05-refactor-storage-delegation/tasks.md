# Tasks: refactor-storage-delegation

## 1. storage_ops! 宏化

- [x] 1.1 `storage/mod.rs` 定义 `storage_ops!` 宏（字段模式 s/o/q/v、fenced/plain、doc 属性转发、tt-muncher 生成代理方法签名；`#[allow(clippy::too_many_arguments)]` 统一）
- [x] 1.2 62 个操作逐项录入操作表（字段清单以 git dispatch 臂为权威逐项核对——实现期发现 `PruneJobCheckpointRecords` 实际只有 `older_than_ms` 单字段，已按权威数据修正；`ReadHubLeaseSnapshot`→`hub_lease_snapshot` 重命名、4 个 `as q` 查询方法、recover_*/prune_* fenced、lease 三方法 plain、5 个 doc 注释保留）
- [x] 1.3 删除被生成的五层手写样板（枚举、nack、label、dispatch 非 Fenced 臂、代理方法 ~1,900 行，mod.rs 6,922→~5,500 行）；**`impl StorageBackend for ControlPlaneStore` 委托层保持手写**——实现期发现 `#[async_trait]` 不处理嵌套宏调用生成的方法（最小复现实证），宏内镜像其脱糖需逐参生命周期编号（宏无法做到），性价比不成立（design.md 已记录）
- [x] 1.4 全量回归：397 lib 测试 + 全部集成测试（two_node_job_smoke/fleet_readiness/bootstrap）绿；clippy/fmt 干净

## 2. DB schema 版本号

- [x] 2.1 `SCHEMA_VERSION = 1` 常量 + `StorageError::SchemaTooNew`（错误信息含两侧版本）；SQLite `ensure_schema_version`（读/拒绝/盖章，幂等）接入 `migrate()` 尾部；PG `cp_schema_meta` 表入 PG_DDL + `ensure_schema_version` 接入 `open()`
- [x] 2.2 `migrate_tool` 零改动即满足规格——源库校验随 `SqliteBackend::open`、目标库盖章随 `PostgresBackend::open` 自动发生
- [x] 2.3 测试：SQLite 两测（首启盖章+重开幂等 / 新版本拒绝）；PG 门控测试（盖章+幂等+拒绝，`ARKFLOW_TEST_POSTGRES_URL` 未设时跳过）
- [ ] 2.4 openspec validate + 归档
