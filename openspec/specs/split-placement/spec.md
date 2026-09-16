# split-placement Specification

## Purpose
Split placement for multi-node Jobs: strategy declaration, deterministic task-level assignment, data-plane capability validation, side-edge co-location constraints, and Hub dispatch of the full task/node map and peer data addresses. Synced from change add-split-placement.

## Requirements
### Requirement: 放置策略 SHALL 由 Job 显式声明且默认共位
`JobSpec` SHALL 支持 `placement: colocated | split`，默认 `colocated`。默认值下放置行为 MUST 与既有连通分量整体落位逐位一致（含"拆边即报错"契约）。

#### Scenario: 旧 spec 反序列化默认共位
- **WHEN** 一份不含 `placement` 字段的存量 Job spec JSON 被反序列化并编译
- **THEN** 放置策略为 colocated，assignment 与升级前一致

### Requirement: split 放置 SHALL 按 task 粒度确定性轮转
`split` 放置 SHALL 按 plan 的物理 task 顺序（sources→operators→sinks）以节点数取模轮转分配；相同输入 MUST 在任意节点推导出相同映射。同 operator 的 subtask SHALL 分布到不同节点（当节点数 ≥ 2 时）。

#### Scenario: 双节点轮转分配
- **WHEN** 一个 3 operator × 2 subtask 的 Job 以 split 放置分配到节点 [A, B]
- **THEN** task 按顺序交替落位 A/B/A/B/A/B，且同一输入重复分配得到相同映射

#### Scenario: 同算子 subtask 跨节点散布
- **WHEN** 一个 keyed operator 有 2 个 subtask 且节点数为 2
- **THEN** 两个 subtask 落在不同节点，key-group 分区跨节点生效

### Requirement: split 放置 SHALL 校验数据面能力
`split` 放置的期望节点集中任一节点缺少数据面端口或 `network_shuffle` capability 时，放置 SHALL 被拒绝（fail-closed），MUST NOT 下发部分 assignment。

#### Scenario: 无数据端口节点拒绝
- **WHEN** split 放置的期望节点集包含一个未申报数据端口的节点
- **THEN** 放置校验失败并指名该节点，不下发任何 job_start 命令

### Requirement: 旁路边 SHALL 保持共位
split 放置下，error 边与 late-event route 边的两端算子 subtask MUST 落在同一节点；违反时放置校验 SHALL 失败，而非运行期图构建报错。

#### Scenario: 跨节点旁路边拒绝
- **WHEN** split 轮转把一个带 late-event route 的 source 与其 route 目标算子分到不同节点
- **THEN** 放置校验失败并指出旁路边跨节点

### Requirement: Hub SHALL 下发全量映射与端口表
split 放置的 job_start 命令载荷 SHALL 包含全量 `task_nodes`（task→node 映射）与 `node_data_ports`（期望节点→数据地址）；每个节点收到的命令只执行自身 assignment，但 MUST 能据载荷推导全部远程边。

#### Scenario: 节点载荷自足
- **WHEN** 节点 A 收到 split Job 的 job_start 命令
- **THEN** 载荷含全量 task_nodes 与相关节点数据端口，A 无需额外查询即可构建含远程边的图
