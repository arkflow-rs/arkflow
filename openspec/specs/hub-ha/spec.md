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
