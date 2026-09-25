# Design: add-stream-join-operator

## Context

内核算子以 `Processor` trait 接入（`process`/`on_watermark`/`close`），多输入链已有支持（`Aligner` + `chain.inputs: Vec<Receiver>`，watermark 取每边最小值转发）。缺的是：(a) 处理器无法区分批次来自哪条入边；(b) join 算子本体。

## Goals / Non-Goals

- Goals：keyed interval join 的够用实现（见 proposal），语义边界诚实记录。
- Non-Goals：temporal/lookup join、非 equi-join、跨节点 shuffle join、独立状态快照、outer join。

## Decisions

1. **输入侧别用元数据列而非新 trait**：给 `Chain` 加 `tags_input_index: bool`（构建期由 plan 是否含 Join 算子决定），链路循环在数据分发前给批次追加 `__meta_input_index`（UInt32）。复用 `__meta_*` 既有约定，处理器零新接口；`__meta_` 列本就是源元数据的既有通道，下游 sql 等组件已习惯忽略/使用它们。
2. **匹配即发射，缓冲只存引用**：`BufferedRow` 持 `(ts, MessageBatchRef, row_index)`，输出时用 `arrow::compute::interleave` 按列聚集原始值——插入零行拷贝，输出保真类型。
3. **有界性双保险**：watermark 逐出（窗口闭合）+ `max_per_key` 容量逐出（防无 watermark 的 processing-time 流无界增长）。
4. **恢复走重放**：不引入 join 状态快照。窗口操作符需要快照是因为聚合状态不可从输入重导（源 offset 回退代价之外的语义）；join 缓冲是输入的确定性函数，回放即重建。at-least-once 语义与内核既有承诺一致。
5. **join 链单线程**：与 stateful 链同规则（`processor_parallelism = 1`），worker 池并发会交错缓冲更新并跨 barrier 切口。
6. **左右定序**：入边声明序 = 侧别序。JobSpec 的 edges 是 Vec（有序），编译确定性保持。

## Risks / Trade-offs

- 两侧 schema 要求稳定：流中变更即报错（诚实快败优于静默错列）。
- 无 watermark 的 processing-time 流只靠 `max_per_key` 界——文档明示建议事件时间源。
- 大 key 倾斜下 `max_per_key` 逐出丢匹配——容量规划属配置责任（与窗口 key 基数同性质）。

## Migration Plan

纯新增能力 + 报错文案更新；此前被拒绝的 Job DAG Join 算子现在可用，无既有用户受影响（该入口一直拒绝）。
