## Why

`optimize-avro-columnar-decode`（#1310，已合入 main）为 Avro 路径落地了列式累积（实测 4.7–8.7x），但 decode 分组中的 **Protobuf 组仍是逐消息单行 batch**：`protobuf_to_arrow` 每消息每字段分配一个单元素 Arrow 数组并重建 `Field`/`Schema`，同批同 schema id 的分配次数为 O(消息数 × 字段数)，且每消息每字段做一次 `get_field_by_name` 名字哈希查找。descriptor→列映射是纯 schema 派生（全字段 nullable），具备与 Avro 相同的批级构造条件。

## What Changes

- `component/protobuf.rs` 新增 `ProtobufBatchConverter`：持有（廉价 Arc 克隆的）`MessageDescriptor`，首条消息的字段遍历中 tee 出列计划（name/字段号/叶类型闭式枚举；unsupported kind 拒绝沿用现状文案），后续消息按字段号取值 append 进 Arrow builders，`finish()` 直出多行 batch。
- `codec/schema_registry.rs` decode 的 `Group::Protobuf` 从 `Vec<RecordBatch>` 改为 converter，组间仍走既有 `normalize_and_concat`。
- 零行为变更：全字段恒 nullable 使 nullability 免疫归一提升；组内行序 = 消息序、proto3 隐式存在性默认值语义（未设置标量 → 默认值而非 null）、错误文案与归因（kind 拒绝在首条消息 `DynamicMessage` 解码之后触发，与逐消息路径同位）均与逐消息路径一致。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `schema-registry-integration`: 新增需求「Protobuf 批级列式解码」——同一次 decode 内同一 Protobuf schema id 的消息 SHALL 累积为单一多行批次（列计划每批计算一次），SHALL NOT 逐消息构造独立单行 RecordBatch；输出与逐消息路径 + 归一合并逐列一致。

## Impact

- `crates/arkflow-plugin/src/component/protobuf.rs`（新增批转换器；单值 `protobuf_to_arrow` 原样保留）
- `crates/arkflow-plugin/src/codec/schema_registry.rs`（`Group::Protobuf` 接线）
- 无公开 API 新增、无依赖、无配置变化；无既有 protobuf 基准场景，性能以 ad-hoc release 计时留档（见 tasks）
