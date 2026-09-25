# hub-ha 变更（Delta）

## ADDED Requirements

### Requirement: PostgreSQL 存储后端设计文档

SHALL 产出阶段 1（SQLite→PostgreSQL）的评审级设计文档，覆盖：backend 抽象与 actor 顺序不变性、17 张 `cp_*` 表的类型/索引映射、`ARKFLOW_HUB_STORAGE` scheme 分派的向后兼容配置面、一次性数据迁移工具、事务与唯一约束语义映射、双后端契约测试策略、风险与决策记录。

#### Scenario: 设计文档可支撑实施立项

- **WHEN** 查阅 postgres-storage-design 设计文档
- **THEN** 包含 backend trait 签名级定义、逐表映射表、迁移工具命令行契约、测试门控方式与实施任务分解
