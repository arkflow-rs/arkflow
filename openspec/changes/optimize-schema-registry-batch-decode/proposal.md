## Why

`schema_registry` codec 的 Avro 与 Protobuf 解码路径仍是"逐消息单行 batch"形态：每条消息每字段分配一个单元素 Arrow array（`vec![Some(v)]`）并重建 `Field`/`Schema`/`RecordBatch`，随后 `normalize_and_concat` 再把 N 个单行 batch 全量拼一遍。这是 `optimize-schema-registry-avro-decode`（2026-10-06 归档）design 中明确延后的 **L3 成本层**——当时建立 avro-decode 基准场景的目的就是为本立项提供数据；现留档数据显示 L1（Arc 缓存）消灭深拷贝后仍有 +19.6%~+22.1% 空间被释放，L3 的分配占比随之更显性（w25 场景 45k rows/s，分配密集）。

## What Changes

- `decode(Vec<Bytes>)` 循环保持现状操作顺序（逐消息:wire 头 → 缓存 → 解码 → 字段遍历），首个 schema id 建立批转换器：schema→Arrow 列映射（字段名、类型、nullability、decimal 元数据）在首条消息的字段遍历中逐字段 tee 导出，后续消息照计划 append 进 Arrow builders，**列式累积为一个多行 batch 后直接返回**，完全跳过 `normalize_and_concat`；中途出现第二个 schema id（同批真实 schema 演进，罕见）时，余下消息切回现状单行路径，前缀多行 batch 与后续单行 batches 一起归一合并——行序、错误顺序、fetch 副作用顺序全部按构造与现状一致。
- 输出逐列复刻现状（含既有怪行为：N≥2 时归一合并把全部列提升为 nullable，N=1 保持扁平映射的 nullability；该怪行为的清理不在本变更内）。
- Avro 路径（`avro_arrow.rs`）与 Protobuf 路径（`component/protobuf.rs`）同步改造；两路径的既有单值转换语义、错误类型与错误文案零变化。
- 逐消息 `GenericDatumReader` 构建（L2）保持不变——apache-avro 公开 API 阻塞，属上游议题。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `schema-registry-integration`: 新增需求「批级列式解码」——同一次 decode 内单一 schema id 的消息 SHALL 累积为单一多行批次（列映射每批计算一次、于首条消息解码成功后构建），SHALL NOT 逐消息构造独立 RecordBatch；输出与既有逐消息拼接语义逐列等价（含 nullability 的消息数依赖、错误优先级与首错归因）；混合 schema id 行为不变。零行为变更的契约固化。

## Impact

- `crates/arkflow-plugin/src/codec/schema_registry.rs`（decode 循环改分组 + 批级调用）
- `crates/arkflow-plugin/src/codec/avro_arrow.rs`（新增批级转换 API，保留单值 API 与全部校验/错误文案）
- `crates/arkflow-plugin/src/component/protobuf.rs`（同上批级化）
- 测试：既有 codec/avro/protobuf 测试全绿为零行为变更验证；新增分组顺序、单组跳过拼接、混合组等价性测试；基准 A/B 用既有 avro-decode 三场景（不新增场景）。
- 无配置、无依赖、无 wire format 变化；合入即生效，回滚 = revert。
