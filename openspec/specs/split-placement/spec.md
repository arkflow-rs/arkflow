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

### Requirement: 放置亲和组 SHALL 共位
`JobSpec` SHALL 推导放置亲和组：对每个 source 按 `input_type`、每个 sink 按 `output_type` 解析核心 builder 注册表并调用其放置亲和钩子，返回相同 key 的算子（含其全部 subtask）构成一个亲和组。注册表中不存在的类型 MUST NOT 贡献亲和也 MUST NOT 使放置失败（该类型无法在任何节点构建，由既有建图路径 fail-closed）。

`split` 放置 SHALL 在轮转分配后执行确定性亲和钉扎：每个亲和组的全部成员任务改派到该组在物理 plan 顺序中首个成员任务所落的节点。钉扎结果 MUST 是「任务顺序 + 节点列表」的纯函数——相同输入在任意节点推导出相同映射；钉扎 MUST 在旁路边校验之前完成，钉扎后仍跨节点的旁路边按既有规则拒绝整份 assignment。

`colocated` 放置的接受结果 MUST 与既有连通分量整体落位逐位一致；亲和校验 SHALL 同样执行，因此配对两端不连通且落到不同节点的 assignment 被拒绝。

Hub 放置期（下发任何 job_start 之前）与 Agent 建图期 SHALL 各校验一次亲和组共位；违反时 SHALL 以指名亲和 key、两端任务与节点的配置错误拒绝，MUST NOT 下发部分 assignment。增量重放置（原位替换失败槽后重算 assignment）SHALL 保持亲和组整体同节点，且幸存节点的 assignment 逐字节不变。

#### Scenario: split 轮转后配对被钉到 source 节点
- **WHEN** 一个 source→operator→sink 的 Job 以 split 放置分配到节点 [A, B]，且 source 与 sink 声明同一亲和 key
- **THEN** 轮转把 source 落 A、operator 落 B、sink 落 A（钉扎到 source 节点），source→operator 与 operator→sink 成为远程边，assignment 被接受

#### Scenario: 钉扎结果确定
- **WHEN** 同一 plan 与同一节点列表在 Hub 与两个 Agent 上各自推导 assignment
- **THEN** 三处得到逐字节相同的 task→node 映射

#### Scenario: 拆分的亲和组在 Hub 被拒绝
- **WHEN** 一份 assignment（手工或陈旧）把亲和组两端任务分到不同节点并提交放置校验
- **THEN** 校验失败，错误指名亲和 key、两端任务与各自节点，不下发任何 job_start

#### Scenario: Agent 对陈旧 task map 兜底
- **WHEN** Agent 收到把亲和组拆到两节点的 task map 并建图
- **THEN** `build_subgraph` 以同类配置错误失败，作业不在该 Agent 启动

#### Scenario: 节点故障后配对一起迁移
- **WHEN** 承载亲和组的节点离线，Hub 以原位替换失败槽的方式增量重放置
- **THEN** 亲和组全部成员落到同一替换节点，其余幸存节点的 assignment 逐字节不变

#### Scenario: 钉扎导致旁路边跨节点时整体拒绝
- **WHEN** 钉扎把某 sink 改派后使一条 error 旁路边的两端落到不同节点
- **THEN** 放置按既有旁路边规则拒绝整份 assignment，而非静默接受

#### Scenario: colocated 下不连通的配对被拒绝
- **WHEN** colocated 放置把两个不连通分量分到不同节点，而一个分量的 source 与另一分量的 sink 声明同一亲和 key
- **THEN** 放置校验以亲和错误拒绝（该组合此前在 Agent 启动期才失败）

#### Scenario: 未注册类型不影响放置
- **WHEN** plan 含一个本进程注册表中不存在的 `input_type`
- **THEN** 亲和推导忽略该 source，放置不因此失败，构建期仍按既有路径报未知类型

