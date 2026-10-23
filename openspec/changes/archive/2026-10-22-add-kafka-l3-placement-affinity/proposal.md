## Why

Kafka L3 exactly-once（`transactional_offsets` 输入 + `offset_commit_group` 输出，源位点折入 producer 事务）目前只在**同进程**配对下可用：输入经进程全局注册表交接消费者组元数据与 ack 前沿（`crates/arkflow-plugin/src/kafka_txn.rs:47-51`），输出在写入期按组名查该注册表（`crates/arkflow-plugin/src/output/kafka.rs:504-516`，查不到即 `no live input frontier in this process`），启动期配对校验在每个进程内独立判定（`crates/arkflow-plugin/src/kafka_txn.rs:179-217`）。分布式 Job 启用 `placement: split` 时，规划器按物理 task 顺序对节点数取模轮转（`crates/arkflow-core/src/job.rs:1075-1097`：`node_ids[index % node_ids.len()]`，之后只校验旁路边共位）——L3 输入是 source（排在最前）、配对输出是 sink（排在最后），两节点以上几乎必然分到不同节点，输入所在节点启动即以"本进程无输出认领该组"失败。规范对此的诚实边界是"跨进程配对不覆盖、显式 fail-closed"（`openspec/specs/exactly-once-output/spec.md:44`），这意味着 **split 放置与 L3 互斥**：想要端到端精确一次就放弃跨节点扩展，反之亦然。

根因不是"元数据无法跨进程"（librdkafka 暴露了组元数据四字段访问器与 `rd_kafka_consumer_group_metadata_new_with_genid` 构造器，可明文重建），而是**前沿（contiguous acked frontier）必须对输出实时可见**——跨节点传递前沿需要新的数据面协议。规划器已有一个完全同构的先例：旁路边（error / late-event route）在 split 放置下 MUST 共位，由 `JobSpec::side_edges()` 从算子配置推导（`crates/arkflow-core/src/job.rs:416-460`）并在 Hub 放置期与 Agent 建图期（`crates/arkflow-core/src/executor/graph.rs:438-441`）双侧校验。把 L3 配对表达为同样的**共位亲和约束**，split 放置就能在不引入任何跨节点协议的前提下保留完整 L3 保证：配对的输入/输出任务钉到同一节点，其间的处理算子仍可跨节点分布。

## What Changes

- **组件声明放置亲和**：`InputBuilder` / `OutputBuilder` 增加带默认实现（`None`）的 `placement_affinity(config)` 钩子；Kafka 输入在 `transactional_offsets: true` 时、Kafka 输出在设置 `offset_commit_group` 时返回同一亲和 key（`kafka-txn:<consumer_group>`）。其他组件不受影响。
- **规划器推导亲和组**：`JobSpec` 新增 `affinity_groups()`，经核心 builder 注册表按 `input_type` / `output_type` 解析钩子，把同 key 的算子归为一组（与 `side_edges()` 同形）。注册表中不存在的类型不贡献亲和（该类型在任何节点都无法构建，fail-closed 由既有建图路径兜底）。
- **split 放置钉扎**：轮转分配后增加确定性亲和钉扎 pass——每个亲和组的全部成员任务（含所有 subtask）改派到该组在 plan 顺序中首个成员所落的节点。结果仍是"任务顺序 + 节点列表"的纯函数，Hub 增量重放置（原位替换失败槽）自动保持配对同节点。
- **双侧 fail-closed 校验**：新增公开的 `validate_affinity_assignments` / `validate_affinity_nodes`，与旁路边校验并列：Hub 放置期（两种策略均校验）与 Agent `build_subgraph` 期各执行一次；跨节点的亲和组以指名组 key、两端任务与节点的配置错误拒绝，不进入运行。
- **错误信息对齐**：`kafka_txn::validate_pairings` 的"本进程无输出认领"错误补充放置提示（分布式部署下该错误意味着 assignment 未经亲和钉扎/校验）。
- **规范与文档**：`exactly-once-output` 诚实边界改写为"分布式 Job 由放置亲和保证配对同节点"；`split-placement` 新增亲和共位需求；`distributed-job-runtime` 放置需求补充亲和约束；中英文 `build/exactly-once.md` 与 `build/distributed-jobs.md` 更新。

无 **BREAKING** 变更：`colocated` 策略的接受结果逐位不变（连通分量整体落位天然满足亲和），仅新增对"配对输入/输出不连通且被分到不同节点"这一从未能成功运行的组合的放置期拒绝；旧 Job spec 反序列化不受影响（亲和由配置推导，不新增 spec 字段）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `exactly-once-output`：「Effectively-once boundary is honestly scoped」需求——跨进程边界从"不覆盖、fail-closed"改为"分布式 Job 由放置亲和把配对钉到同一节点；被拆分的配对在 Hub 放置期与 Agent 建图期均被拒绝"；「Kafka 输出 SHALL 支持事务内源位点提交（L3）」需求补充放置亲和声明场景。
- `split-placement`：新增「L3 事务配对 SHALL 共位」需求——亲和组推导、split 轮转后的确定性钉扎、增量重放置保持、手工/陈旧 assignment 的双侧拒绝。
- `distributed-job-runtime`：「Compute nodes SHALL execute fenced task attempts」需求的放置约束句补充"组件声明的放置亲和组 MUST 共位"。

## Impact

- **arkflow-core**：`input/mod.rs`、`output/mod.rs`（trait 默认方法 + 注册表查询）；`job.rs`（`AffinityGroupSpec`、`affinity_groups()`、`assignments_split` 钉扎 pass、`validate_affinity_*`）；`executor/graph.rs`（`build_subgraph` 校验）。公共 API 仅新增带默认实现的 trait 方法与两个与旁路边校验对称的公开校验函数，符合 `core-api-surface` 最小面原则；放置逻辑本就住在 core，不引入 Hub 专属逻辑。
- **arkflow-plugin**：`input/kafka.rs`、`output/kafka.rs` 的 builder 实现钩子；`kafka_txn.rs` 错误文案。
- **arkflow-server**：Hub 放置路径（`hub/placement.rs`、`api/jobs.rs`、`hub/checkpoint.rs`）无需改代码——校验随 `assignments_for_nodes` 生效；需补两节点 split + L3 配对的放置测试。
- **规范/文档**：三份 spec 增量；`docs/docs/build/exactly-once.md`、`docs/docs/build/distributed-jobs.md` 及 zh-Hans `current` 树对应页。
- **不新增依赖**；不触碰 WAL / ack / checkpoint 路径；不改变 `kafka_txn` 注册表的进程内语义。
