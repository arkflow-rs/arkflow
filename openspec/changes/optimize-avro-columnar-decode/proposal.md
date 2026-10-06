## Why

`optimize-schema-registry-avro-decode`（PR #1309，已归档）消灭了 Avro 解码热路径的 L1（每消息 schema 深拷贝，实测 +20%），其基准数据同时暴露了剩余成本的主导项 **L3**：decode 循环对每条消息产出一个单行 `RecordBatch`——每个字段一次全新 Arrow array 分配、每消息一次 batch 构造——随后 `normalize_and_concat` 把全部数据再拷贝一遍。吞吐随 schema 宽度线性下降（w5 208k → w25 45k → w100 9.8k rows/s）正是"每字段每消息固定分配成本"的形状。列式累积（一批消息共享 schema id 时直接追加进各列 builder）把分配次数从 O(消息数 × 字段数) 降到 O(字段数)，是数据已论证的下一跳。

## What Changes

- `avro_arrow.rs` 新增**列式累积器**：按 writer schema 的字段列表预构建对应的列 builder（`Int64Builder`/`StringBuilder`/…，nullable union 为 `Option` 追加），leaf 类型集与现有扁平映射完全一致（嵌套 record/array/map 仍构建期拒绝）；逐值追加，`finish()` 产出一个多行 `RecordBatch`。
- `schema_registry.rs` decode 循环**按 schema id 分组**消息：同组消息共用一个累积器与 `CachedSchema`，每组产出一个多行批次；组间（真实 schema 演进混批）仍走既有 `normalize_and_concat` 合并。
- `avro_to_arrow` 保留为累积器的单消息薄封装（推一条 + finish），公开签名与既有测试兼容。
- 已知语义微差（如实声明）：多 schema id 混批时，并集列序由"跨消息首次出现"变为"跨分组首次出现"——单 id 批（绝大多数流量）逐字节不变；`schema-registry-integration` 的「多版本 schema 解码」requirement 措辞同步。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `schema-registry-integration`: 「多版本 schema 解码」requirement 的列序表述更新——并集列序取首个 schema id 分组内首条消息的字段顺序（单版本批行为不变）。

## Impact

- `crates/arkflow-plugin/src/codec/avro_arrow.rs`：新增累积器类型与逐 leaf 类型的 builder 追加臂（与现有 `leaf_to_arrow`/`null_column` 的类型映射一一对应）；`avro_to_arrow` 改为薄封装。
- `crates/arkflow-plugin/src/codec/schema_registry.rs`：decode 循环改为按 id 分组累积。
- 无公开 API 新增、无依赖变化、无配置变化；protobuf 路径与 encode 路径不动。
- 性能由既有 `avro-decode-w5/w25/w100` 基准场景直接度量，release 前后对比留档。
