## Context

`SchemaRegistryCodec::decode`（`schema_registry.rs:298`）现状：逐消息 `parse_wire_format` → `resolve_cached`（Arc 命中，L1 已闭环）→ `avro_to_arrow` / `protobuf_to_arrow` 产出**单行 RecordBatch**，循环结束再 `normalize_and_concat` 全量拼接。每条消息的每字段成本：`vec![Some(v)]` 堆分配 → 单元素 Arrow array（validity/values/offsets buffer）、`Arc<Field>`（含 name String）、`Schema::new`、`RecordBatch::try_new`；拼接阶段再对 N 个单行 batch 做一次全量复制。这是上轮 design 标注的 **L3 层**（L2 reader 构建被 apache-avro 公开 API 阻塞，维持不动）。留档基线（2026-10-06，release，count=400k ×3 best-of）：w5 207k / w25 45.5k / w100 9.8k rows/s（Arc 化后值）。

Avro 侧 schema→Arrow 映射（`avro_arrow.rs`）与 Protobuf 侧 descriptor→列映射（`component/protobuf.rs`，全部字段 nullable）都是**纯 schema 派生**：字段集、类型、nullability、decimal precision/scale 不依赖消息值——同一 schema id 的全部消息共享同一映射，具备批级构造条件。

现状行为盘点中两个易被"零行为变更"声明忽略的暗点（本设计必须显式复刻或绕开，均已对照 `batch_merge.rs` 核实）：

1. **输出列 nullability 随消息数变化**：`normalize_and_concat` 对新列无条件 `with_nullable(true)`（batch_merge.rs:48），单批时直通 clone（batch_merge.rs:37）。即现状 decode 输出:N=1 时非 union 字段 nullable=false;N≥2 时**所有列**（含非 union 字段）被提升为 nullable=true。
2. **错误触发的精确顺序**：两层级——批内按消息序，任何消息 j 的错误（wire 头、fetch、解码、转换）先于消息 k>j 的错误；消息内按字段序交错（schema 检查→值检查→下一字段），即字段 i 的 schema 错误先于字段 i+1 的任何错误，但**不先于**更早字段的值错误。

## Goals / Non-Goals

**Goals:**

- 消灭 L3：同一次 decode 内单一 schema id（占绝大多数）的消息累积为单一多行批次（Arrow builders 列式构造），schema→Arrow 映射每批计算一次；单一 id 跳过 `normalize_and_concat`。
- Avro 与 Protobuf 两条路径同步批级化。
- 零行为变更（按构造等价）：输出 batch（含列 nullability 的 N 依赖怪行为）、错误类型、错误文案、批内与消息内的错误触发顺序、fetch 副作用顺序、输出行序与现状逐消息路径完全一致。

**Non-Goals:**

- L2 reader 复用（上游 `apache-avro` API 阻塞，路径是向上游提 owned-reader PR）。
- 嵌套 record/array/map 支持（扁平映射约定不变，拒绝文案不变）。
- 新增基准场景（avro-decode w5/w25/w100 已就位，直接用于 A/B）。
- `CachedSchema` 结构变化（不把 Arrow 列计划塞进 schema 缓存）。
- **nullability 怪行为清理**（N≥2 全列提升为 nullable 是 `batch_merge` 的既有行为，本变更原样复刻；是否清理另行立项）。
- 混合 schema id 批次的性能优化（优雅降级回现状路径，见决策 1）。
- protobuf `DynamicMessage::decode` 内部行为（descriptor clone 为廉价 Arc 克隆，非成本项）。

## Decisions

1. **动态顺序处理 + tee + 优雅降级：不做任何预扫描。**
   decode 循环保持现状的操作顺序逐字不变（逐消息:wire 头解析 → `resolve_cached` → reader 构建 + `read_value` → 字段遍历）。首个 schema id 出现时建立批转换器（plans + builders）；同 id 消息逐条 append；**中途出现第二个 id 时，余下消息切回现状单行路径逐条转换，最终 `normalize_and_concat([前缀多行 batch, 后续单行 batches])`**——前缀 batch 位于拼接列表头部，行序天然保持消息序，无双解码。理由：错误顺序、副作用顺序（fetch 时机）、行序全部**按构造等价**——循环形状与现状相同，差异只有"检查结论被记住"与"值进 builder 而非单元素数组"。备选（均被否）：**预扫描 wire header 后分派**——全批头校验被提到 fetch/解码之前，位置靠后的坏头错误会抢在更早消息的 fetch/解码错误之前返回，错误文本分歧；先分组再逐组处理——行序变为按 id 聚集、错误归因漂移到首现组；分组 + gather 重排——为罕见路径引入索引重排复杂度。
2. **plans 以 tee 方式在首条消息的字段遍历中逐字段导出。**
   首条消息的逐字段处理顺序为 [schema 检查（同现状、同文案）→ 结论记入 plans → 值检查 → append]，即现状 `avro_value_to_arrow` 循环的原形状外加记录——由此消息内的字段级交错顺序（字段 i 的 schema 错误不先于更早字段的值错误）天然保持。后续消息跳过 schema 级检查：schema 确定、首条已通过，现状代码对后续消息的同类检查必然通过，跳过不可观察。Protobuf 同构（首条 `DynamicMessage::decode` 成功后的字段遍历中 tee;unsupported kind 走现状 catch-all 拒绝文案）。
3. **输出 nullability 逐分支复刻现状（修正后仅存的显式复刻点）。**
   单一 id 且 N≥2：输出 schema 全列 `with_nullable(true)`（复刻 `normalize_and_concat` 的无条件 union 提升，仅改 Field 的 nullable 标志，数组位图与值不变）;N=1:保持 plans nullability（现状单批直通 clone 的行为）。混合 id 路径经多批 normalize 自动获得全 nullable，与现状一致，无需处理。Protobuf 全字段本就 nullable=true，天然一致。
4. **列计划（plans）为闭式枚举，builders 为对应闭式枚举包装。**
   Avro:plans 元素含 name、nullable、叶类型枚举（含 union unwrap、decimal (p,s)、timestamp 时区）;按 plans 构造 builder 枚举（`PrimitiveBuilder` / `BooleanBuilder` / 字节类 builder / `Decimal128Builder` …），逐消息 append、`finish` 出列。decimal 的 `with_precision_and_scale` 与 timestamp 的 `with_timezone` 在 finish 后作用于数组（与现状单元素路径同构）。Protobuf 同构（descriptor fields() 一次遍历;现状每消息每字段的 `get_field_by_name` 查找随 plans 一并消除）。备选:`Box<dyn ArrayBuilder>` + downcast（被否——每字段每消息 downcast，类型不安全）;两遍法（先存全部 `AvroValue` 再按列聚 `Vec<Option<T>>` 构造，被否——多一层中间分配，内存峰值更高；若 builder 枚举实现膨胀失控可作为等价输出的回退实现，属实现自由度）。
5. **单值转换 API 保留。**
   `avro_to_arrow` / `protobuf_to_arrow` 单消息签名保留（测试与潜在调用方使用），实现可退化为"批转换器的 N=1 特例"或保持原样——以测试对齐为准，避免无谓重复代码。
6. **性能验证沿用上轮方法。**
   release 测试二进制，既有 avro-decode 三场景，count=400k 单轮 ×3 取最优，与父提交对照；数据记入 tasks 备注。不断言性能数值（benchmark-suite 语义）。

## Risks / Trade-offs

- [等价性论证依赖"schema 级检查确定性"] → 检查是 schema 的纯函数、循环内 schema 恒定，首条通过即全通过;该不变量写入决策 2 并由等价性测试间接覆盖（含字段级错误顺序专项测试）。
- [nullability 复刻被后人当 bug"顺手修掉"] → 等价性测试显式断言 N=1 与 N≥2 两分支的列 nullability，与现状逐消息输出逐列相等；怪行为的清理留待独立立项。
- [builder 值正确性（decimal 精度修饰、timestamp 时区在 finish 后作用于数组）] → 任务 1.4 全类型覆盖测试断言列类型（含 `Decimal128(p,s)`、`Timestamp(_, Some("UTC"))`）与值序。
- [混合 id 降级路径复杂度] → 降级即"余下消息走现状代码"，前缀 batch 与单行 batches 共存于一次 concat;混合 id 行序测试锁定（输出行序 = 消息序）。
- [builder 枚举 ×18 种叶类型的代码膨胀] → 既有 `null_union_rows_cover_every_supported_leaf_type` / `every_leaf_type_maps_to_its_arrow_column` 全类型覆盖测试改造为批级断言（多行、含 null 混排），文案类测试（mismatch/unsupported/nested/union 分支）保持单值与批级双跑。
- [内存峰值上升] → 不会:快路径仅持有原始 Bytes + 增长中的 builders + 逐消息即弃的 AvroValue，较现状（原始 Bytes + N 个单行 batch + 合并输出三层并存）更低;混合路径的额外持有仅为前缀 batch。
- [提升不达预期（如 w5 不显著）] → 零行为变更 + 分配路径严格减少，最坏持平；如实留档，不以数值门禁合入。

## Migration Plan

零配置、零 schema、零 wire format 变化；合入即生效，回滚 = revert。

## Open Questions

（无——builder 实现形态在决策 4 已留回退路径，属实现自由度。）
