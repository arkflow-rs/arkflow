# Capability: Hub 高可用

## Purpose

Hub 当前为单实例部署（SQLite 单写者、无故障转移）。本 capability 记录 HA 演进的决策与蓝图：`architecture.md` 是选项调研（选主机制、存储后端、Agent 重连、迁移路径），`postgres-storage-design.md` 是阶段 1（存储迁移）的实施蓝图。

## Requirements

### Requirement: HA 架构设计文档

SHALL 产出架构设计文档，覆盖 leader election 机制对比（Raft / 外部协调服务 / DB 租约）、存储后端选项（SQLite WAL / PostgreSQL / etcd）、Agent 重连策略、数据面不受影响的保证。

#### Scenario: 设计文档覆盖全部 HA 架构决策维度

- **WHEN** 查阅 HA 架构设计文档
- **THEN** 包含架构选项对比表、推荐方案及理由、迁移路径说明

### Requirement: PostgreSQL 存储后端设计文档

SHALL 产出阶段 1（SQLite→PostgreSQL）的评审级设计文档，覆盖：backend 抽象与 actor 顺序不变性、17 张 `cp_*` 表的类型/索引映射、`ARKFLOW_HUB_STORAGE` scheme 分派的向后兼容配置面、一次性数据迁移工具、事务与唯一约束语义映射、双后端契约测试策略、风险与决策记录。

#### Scenario: 设计文档可支撑实施立项

- **WHEN** 查阅 postgres-storage-design 设计文档
- **THEN** 包含 backend trait 签名级定义、逐表映射表、迁移工具命令行契约、测试门控方式与实施任务分解

### Requirement: PostgreSQL 存储后端

`ARKFLOW_HUB_STORAGE` 以 `postgres://` 开头时，Hub SHALL 使用 PostgreSQL（sqlx PgPool）作为控制面存储；裸路径 SHALL 保持 SQLite 行为逐字节不变。两种 backend SHALL 共享同一命令面与 FIFO 顺序语义（actor 串行保持）；17 张 `cp_*` 表的 PG DDL SHALL 幂等且在启动时执行；连接失败 SHALL 使 Hub 启动快败。

#### Scenario: scheme 分派向后兼容

- **WHEN** `ARKFLOW_HUB_STORAGE` 分别为裸路径与 `postgres://` URL
- **THEN** 前者行为与现状一致，后者连接 PG 并执行幂等 DDL

#### Scenario: 两种 backend 通过同一契约套件

- **WHEN** 以同一命令序列分别驱动 SQLite 与 PG backend
- **THEN** 两者产出一致的存储状态与查询结果

### Requirement: SQLite 到 PostgreSQL 一次性迁移

SHALL 提供 `arkflow-server migrate --from sqlite:<path> --to postgres:<url>`：按依赖序逐表分块拷贝、重置 IDENTITY 序列、行数对账校验，不一致时非零退出。

#### Scenario: 迁移后行数对账一致

- **WHEN** 对含全部 17 表数据的 SQLite 库执行迁移
- **THEN** PG 端各表行数与源一致且抽样主键可查
