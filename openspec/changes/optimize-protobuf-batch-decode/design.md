## Context

main 上的 decode（#1310 后）按 schema id 分组：Avro 组走 `AvroArrowAccumulator` 列式累积，**Protobuf 组仍是 `Vec<RecordBatch>`——逐消息 `protobuf_to_arrow`**。protobuf 单值路径的成本形态与改造前的 Avro 相同：每消息每字段 `vec![v]` 单元素数组（Boolean/Int64/…/String/Bytes 各自的 validity/values/offsets buffer）、每消息重建 `Field`（含 name String）与 `Schema`、每消息每字段一次 `get_field_by_name` 名字哈希查找。descriptor 全字段 nullable（缺字段 → 默认值/null 列）的约定使列映射成为纯 schema 派生。

## Goals / Non-Goals

**Goals:**

- Protobuf 组列式累积：列计划每批计算一次，builders 逐消息 append，`finish` 直出多行 batch。
- 零行为变更（相对 main 现状）：输出逐列一致（含 proto3 隐式存在性默认值语义）、组内行序、错误文案与归因。

**Non-Goals:**

- Avro 路径（已由 #1310 `optimize-avro-columnar-decode` 落地，本变更不触碰）。
- 新增 protobuf 基准场景（benchmark-suite 场景集不变；性能以 ad-hoc release 计时留档）。
- `protobuf_to_arrow` 单值 API 变化（processor 路径与测试继续使用）。
- proto3 optional/oneof/repeated 等不支持字段的放开（unsupported kind 拒绝文案不变）。

## Decisions

1. **converter 持有 `MessageDescriptor`（值语义），无生命周期参数。**
   decode 的分组状态 `Group::Protobuf(_)` 需要跨循环存活，而 `&MessageDescriptor` 只能借用自每轮消息的局部 `Arc<CachedSchema>`（自引用问题）；`MessageDescriptor` 本身是廉价 Arc 克隆（单值路径每消息已在 `descriptor.clone()`），converter 持有一份即可，接入零借用开销。
2. **列计划在首条消息的字段遍历中 tee 导出；后续消息按字段号取值。**
   首条消息逐字段 [kind 分派（unsupported 拒绝文案沿用现状）→ 计划 tee → 值提取 append]，与 `protobuf_to_arrow` 的字段循环同形；后续消息 `get_field_by_number(plan.number)` 取值 append，消除逐消息逐字段的名字哈希查找。kind 分派对后续消息必然通过（descriptor 确定、首条已通过），跳过不可观察——与逐消息路径在相同位置以相同文案触发错误。
3. **全部列 nullable，nullability 免疫归一语义。**
   protobuf 列恒 nullable=true，无论单批直通（clone）还是多组归一提升，输出 nullability 不变——不存在 Avro 路径那个 N 依赖怪行为的复刻问题。
4. **组间仍走既有 `normalize_and_concat`。**
   单一 protobuf 组经单批直通（廉价 clone），混合组经并集归一——与 main 现状的 protobuf 组行为逐列一致。

## Risks / Trade-offs

- [builder 值正确性] → 单元等价性测试（批级 vs 逐消息+归一合并逐列相等）覆盖全部 10 种 kind；codec 级累积测试断言行序与 nullable。
- [proto3 隐式存在性语义漂移（未设置标量 → 默认值 vs null）] → 等价性测试显式覆盖未设置字段行为。
- [性能无既有场景度量] → ad-hoc release 计时（逐消息 vs 批级）留档于 tasks，数字不进 spec/CI（benchmark-suite 语义）。

## Migration Plan

零配置、零 schema、零 wire format 变化；合入即生效，回滚 = revert。

## Open Questions

（无）
