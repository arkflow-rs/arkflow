## Why

Hub 当前为单实例部署（SQLite 单写者、无故障转移），控制面不可用会导致所有 Agent 失去管理面但数据面继续运行（PLANNING 5.8）。需要前置调研评估 HA 架构选项。

## What Changes

- 产出架构设计文档（`openspec/specs/hub-ha/`），覆盖：leader election 机制对比（Raft / 外部协调 / DB 租约）、存储后端选项（SQLite WAL 模式 / PostgreSQL / etcd）、Agent 重连策略、数据面不受影响的保证。
- 不做代码实现（纯设计文档 + 决策记录）。

## Capabilities

### New Capabilities

- `hub-ha`: Hub 高可用架构设计。

## Impact

- 文档产出：设计文档含架构选项对比表、推荐方案、迁移路径。
