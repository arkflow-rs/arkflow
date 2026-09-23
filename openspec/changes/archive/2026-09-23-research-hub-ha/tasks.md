# Tasks: research-hub-ha

## 1. 前置调研

- [x] 1.1 架构选项调研：leader election 机制（Raft/外部协调/DB 租约）对比矩阵
- [x] 1.2 存储后端评估：SQLite WAL 模式 vs PostgreSQL vs etcd 的权衡分析
- [x] 1.3 Agent 重连策略设计：Hub 故障时 Agent 数据面行为、重连超时、新 Hub 发现机制
- [x] 1.4 设计文档产出：openspec/specs/hub-ha/ 架构设计文档（含架构图、决策记录、迁移路径）

## 2. 验证与收尾

- [x] 2.1 `openspec validate research-hub-ha` 通过；PLANNING.md 更新 HA 状态
