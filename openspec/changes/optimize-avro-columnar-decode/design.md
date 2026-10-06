## Context

成本栈现状（PR #1309 后）：L1（每消息 schema 深拷贝）已消灭；L2（`GenericDatumReader` 每消息构建名字表）被 apache-avro 公开 API 阻塞（`decode_internal` 私有、builder 只收借用型 `ResolvedSchema`）；**L3 成为主导**——每消息每字段一个单元素 array 分配 + 单行 `RecordBatch` 构造 + `normalize_and_concat` 全量重拷。基准（2026-10-06，本机）：w5 208k / w25 45k / w100 9.8k rows/s，吞吐随宽度线性下降。

现有结构：`decode` 循环逐消息 `avro_to_arrow(&schema, payload)` → 单行批次收集 → `normalize_and_concat`。`avro_value_to_arrow` 按字段走 `field_to_arrow`/`leaf_to_arrow`/`null_column`，每个 leaf 一个 `XxxArray::from(vec![...])`。

## Goals / Non-Goals

**Goals:**

- 同 schema id 的批内消息**零中间批次**：逐值追加进预构建的列 builder，一次产出多行 `RecordBatch`。
- 输出 schema 与现有映射**逐字段等价**（类型、nullable、字段顺序按 writer schema）。
- 多版本混批语义保持：组间 `normalize_and_concat` 并集合并；唯一微差（列序按分组而非按消息）显式进 delta spec。
- 既有 `avro_to_arrow` 调用面（测试、可能的单消息路径）不破坏。

**Non-Goals:**

- L2 reader 复用（上游 API 阻塞，Non-goal 边界不变）。
- protobuf 解码路径（`MessageDescriptor` 走 prost-reflect，另议）。
- encode 路径、wire format、配置面。
- 嵌套结构支持（维持构建期拒绝）。

## Decisions

1. **累积器形态：`AvroArrowAccumulator { fields: Vec<Field>, builders: Vec<Box<dyn ArrayBuilder>> }`，构造期从 writer schema 建好 builder，`push(schema-backed value) -> Result`，`finish(num_rows) -> RecordBatch`。**
   备选：复用 arrow 的 `RecordBatchOptions`/窄化路径（无此机制）；或逐消息仍建单行批次但重用 builder（仍有一次 batch 构造 + 无法消除 concat 重拷，被否）。
2. **类型分派复用现有映射表**：每个 leaf 臂（Boolean/Int32/Int64/Float32/Float64/Utf8/Binary/Date32/Time32/Time64/Timestamp(Micro/Milli, tz)/Decimal128 + `[null, T]` union）在累积器 `push` 内对应一个 builder 追加臂，类型集与 `leaf_to_arrow`/`null_column` 一一对应——新增映射或漏映射在构建期以与现有相同的错误文案拒绝。
3. **codec 分组：decode 循环内 `HashMap<u32, (Arc<CachedSchema>, AccumulatorState)>`？否——顺序语义要求按首次出现分组。** 用 `Vec<(u32, 累积器)>` 保持首次出现顺序 + 小线性查找（schema id 数在批内通常为 1，线性查找零开销）；组顺序 = 首次出现顺序，喂给 `normalize_and_concat` 后列序语义即"跨分组首次出现"。
4. **`avro_to_arrow` 保留为薄封装**（构造累积器 → push 一条 → finish），既有 22 个 avro 测试零修改通过即证明映射等价。
5. **空批与失败语义不变**：单条 push 失败整批报错（fail 模式）；skip 模式属 JSON codec 路径，与 Avro 无关。

## Risks / Trade-offs

- [映射臂遗漏某 leaf 类型] → 构建期按 schema 校验全字段可累积，与现有构建期拒绝同路径；既有全类型测试（含 `every_leaf_type_maps_to_its_arrow_column`、`null_union_rows_cover_every_supported_leaf_type`）迁移为对累积器的直测。
- [列序微差影响下游] → 仅多 id 混批可见；spec delta 如实声明，单 id 批逐字节不变。
- [Decimal128 精度/时区行为漂移] → 追加臂直接使用与现路径相同的 `with_precision_and_scale`/tz 常量，测试断言类型字符串逐字段相等。
- [基准提升不及预期] → 基准场景已在，release 前后对比留档；若 w100 提升不显著，数据留档供后续（L2/上游）决策。

## Migration Plan

零配置、零 wire 变化；合入即生效，回滚 = revert。

## Open Questions

（无——实现自由度仅剩 builder 容量提示等细节。）
