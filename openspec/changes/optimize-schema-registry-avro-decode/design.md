## Context

schema_registry codec 的 Avro 解码路径（`schema_registry.rs::decode` → `avro_arrow.rs::avro_to_arrow`）每条消息的成本栈，2026-10-06 对照 apache-avro 0.22 源码核实：

- **L1** `resolve_cached` 命中路径 `cached.clone()`：整树深拷贝。`apache_avro::Schema` 无 Arc（`RecordSchema { fields: Vec<RecordField>, lookup: BTreeMap<String, usize>, … }` 全递归克隆）。
- **L2** `GenericDatumReader::builder(schema).build()`：`ResolvedSchema::try_from` 每消息重建名字表 HashMap。本仓库的扁平 Arrow 映射拒绝嵌套 record/array/map，故成本为 O(顶层字段数) 的有界开销（"大 schema" 只能是"宽 schema"）。
- **L3** `avro_value_to_arrow`：每字段每消息分配一个全新单元素 Arrow array。

上游 API 约束（决定本设计的边界）：

1. `decode_internal` 为 `pub(crate)`——绕过 reader 直调解码不可行。
2. `GenericDatumReader<'s>` 的 builder 只接受**借用型** `ResolvedSchema<'s>`，无法注入预解析名字表——reader 复用在公开 API 层走不通。
3. crate 自带 `ResolvedOwnedSchema`（ouroboros 自引用，拥有 schema + 名字表），但 `GenericDatumReader` 消费不了它。

现有基准设施：`crates/arkflow-plugin/src/benchmark.rs`（`ScenarioResult` 模型、`run_suite` 汇总、markdown/JSON 双报告），`crates/arkflow/examples/benchmark.rs` 为公开入口；`benchmark-suite` spec 要求全部场景自包含（无网络、无外部服务）。

## Goals / Non-Goals

**Goals:**

- 消灭 L1：schema 缓存改 `Arc<CachedSchema>`，命中路径从"整树深拷贝"降为"引用计数递增"。
- 建立 `avro-decode` 基准场景：离线、可复现、走真实 `SchemaRegistryCodec::decode` 路径，产出前后可对比的吞吐数据，为 L2（reader 复用，需上游配合）与 L3（列式累积解码）的后续立项提供依据。

**Non-Goals:**

- L2 reader 复用（apache-avro 公开 API 阻塞；如基准证明显著，路径是向上游提 owned-reader 能力的 PR，不在本仓库强改）。
- L3/L4 列式累积解码（结构性优化，独立立项，视基准数据）。
- protobuf 解码路径改动（`MessageDescriptor` 本身廉价克隆，无此问题）。
- 任何行为、错误文案、spec 契约变化（`schema-registry-integration` 的「按 schema id 缓存」语义不变）。

## Decisions

1. **`DashMap<u32, Arc<CachedSchema>>`，`resolve_cached` 返回 `Arc<CachedSchema>`。**
   备选：维持 clone（现状，被本变更消灭）；缓存 `ResolvedOwnedSchema`（被否——reader 消费不了，白带一层）。调用点 decode 循环以 `&*cached` 借用后传入 `avro_to_arrow(&schema, …)`，签名不变。
2. **基准走真实 codec 路径 + 离线 in-memory resolver。**
   场景内构造实现 `SchemaResolver` 的本地注册表（预装各宽度 schema），`SchemaRegistryCodec::new(None, resolver, None)` 后循环 `decode`。备选：直调 `avro_to_arrow`（被否——绕过了缓存命中路径，测不到 L1）；wiremock 假 registry（被否——违反 benchmark-suite 自包含要求）。fetch 路径本就不是被测成本（预热后全命中）。
3. **矩阵：schema 宽度 5/25/100 字段 × 固定批大小（1000 消息/次 decode）。**
   三个场景行（`avro-decode-w5/w25/w100`，unit=rows=消息数），宽度是主要变量；批大小固定以约束默认参数总时长（spec 要求数十秒内完成）。字段类型混合覆盖支持的叶类型（int/long/string/double/boolean/nullable union），真实且让 L3 的分配成本有代表性。
4. **消息预编码在测量外。**
   setup 阶段用 `GenericDatumWriter` 把 N 条 `Value::Record` 编成 Confluent wire format（magic + id + payload），测量只计 `decode` 调用；warmup + 多轮取最优沿用现有 harness 语义。
5. **不断言性能数值。**
   沿用 `benchmark-suite`「报告 SHALL NOT 对吞吐做通过/失败断言」；前后对比由开发者在变更前后各跑一次自行完成（本次变更自身即第一组对照）。

## Risks / Trade-offs

- [Arc 间接层] → 与深拷贝相比可忽略；protobuf 变体无影响。
- [基准数字随机器波动] → 套件既定语义是相对对比而非绝对门禁；报告字段与格式稳定，可复现。
- [in-memory resolver 与真实 REST resolver 漂移] → resolver 是生产同一 trait；fetch 不在被测路径（缓存命中），漂移不影响测量对象。
- [宽 schema 100 字段场景拉长默认运行] → 行数与其他场景对齐、仅宽度变化；实测如超时预算则回调宽度上限，不破坏「数十秒」契约。

## Migration Plan

零行为变更：合入即生效，回滚 = revert。无配置、无 schema、无 wire 格式变化。

## Open Questions

（无——矩阵参数在实现时按运行时长微调，属实现自由度。）
