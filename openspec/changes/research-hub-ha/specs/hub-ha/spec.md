# hub-ha 变更（Delta）

## ADDED Requirements

### Requirement: HA 架构设计文档

 SHALL 产出架构设计文档，覆盖 leader election 机制对比（Raft / 外部协调服务 / DB 租约）、存储后端选项（SQLite WAL / PostgreSQL / etcd）、Agent 重连策略、数据面不受影响的保证。

#### Scenario: 设计文档覆盖全部 HA 架构决策维度

- **WHEN** 查阅 HA 架构设计文档
- **THEN** 包含架构选项对比表、推荐方案及理由、迁移路径说明
