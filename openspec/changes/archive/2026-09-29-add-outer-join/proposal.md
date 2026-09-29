# add-outer-join

## Why

统一内核的 join 算子只支持 inner join(`crates/arkflow-core/src/executor/join.rs:9` 模块文档自述 "Matched pairs are emitted as they arrive (inner join, at-least-once)",`JoinOperatorConfig` 无 join 类型字段),watermark 淘汰的未匹配行被静默丢弃(`join.rs:508-509` `evict` 只删不射)。对 enrichment 类管道(订单流补档案、事件流补维属性)这意味着**未匹配的数据无声丢失**——outer join 是这类场景的硬需求,也是 2026-09-28 能力完备性深挖中评定的最重算子缺口(剩余形态中性价比最高的一项:发射时机复用现有淘汰边界,内核机制零新增)。

顺带修复一个相邻体验缺陷:join 侧别按生产者身份解析为**单个**通道索引(`join.rs:206-221`),任一侧上游算子 parallelism > 1 时,其余子任务的批次在**运行期**才撞上 `join received a batch from input {tag} which is neither the declared left nor right producer`(`join.rs:490-495`)——图构建期不拒绝、作业启动后第一波数据才失败。应前移为构建期显式校验。

## What Changes

- `JoinOperatorConfig` 新增 `join_type` 字段,取值 `inner`(缺省,行为逐位不变)| `left_outer` | `right_outer` | `full_outer`,serde 带缺省保持既有配置向后兼容。
- outer 语义:watermark 淘汰(`timestamp + window_ms + ttl_ms` 过界)时,outer 侧的未匹配行**发射**而非丢弃;已匹配过的行不重复发射。
- 未匹配行的对侧列全为 null(Arrow `new_null_array`),输出 schema 对 outer 侧强制 `nullable = true`(现状直接克隆源字段,`join.rs:458-473`)。
- 容量淘汰(`max_per_key` 弹最旧)在 outer 模式下同样发射未匹配行——发射时无 watermark 保证,属 at-least-once 附属语义,spec 显式声明。
- 构建期校验:join 每侧的生产者必须解析为恰好一个通道(上游 parallelism > 1 的侧在图构建期报错并给出指引),替代运行期才失败的现状。
- 配置校验:outer 模式要求两侧声明事件时间列(或回退列存在)——未匹配发射由 watermark 驱动,无 watermark 则永不发射。
- 示例(`examples/` + manifest 注册)与文档(en/zh)同步。

## Non-goals

- temporal / lookup join(维表广播或外部查询形态)——独立后续 change。
- 非 equi-join、cross join。
- **支持**多子任务侧喂入 join(本变更只做构建期显式拒绝;支持需侧别解析改索引集合,随 temporal join 一起评估)。
- join 状态快照进 StateBackend(维持"checkpoint 重放重建、算子不快照"契约)。
- retract/changelog 输出语义(outer 未匹配行发射后不可收回)。
- 跨节点 shuffle join。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `stream-join-operator`:新增 outer join 需求(join_type 语义、未匹配发射时机、null 列与 nullable schema、容量淘汰语义、事件时间前置)与构建期侧别校验需求。

## Impact

- `crates/arkflow-core/src/executor/join.rs` — 核心改动面:`join_type` 字段、`on_watermark` 淘汰收集+发射、null 列装配、schema nullable。
- `crates/arkflow-core/src/job.rs` — `JoinOperatorConfig` 校验扩展(join_type 合法值、outer 事件时间前置)。
- `crates/arkflow-core/src/executor/graph.rs` — join 构建期单通道校验(现 `with_input_producers` 调用点附近)。
- `examples/` + `docs/reference/example-manifest.json`、`docs/docs/`(en/zh)组件与概念文档。
- 无破坏性变更:未声明 `join_type` 的既有配置行为逐位不变。
