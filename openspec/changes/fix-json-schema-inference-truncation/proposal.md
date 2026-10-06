# Proposal: fix-json-schema-inference-truncation

## Why

JSON 解码的 schema 推断只读取**第一条**记录：`crates/arkflow-plugin/src/component/json.rs:28` 调用 `infer_json_schema(&mut cursor_for_inference, Some(1))`，随后用该 schema 解码全部行（arrow-json 默认 `strict_mode=false`）。两个静默错误后果：

1. **数值截断**：首行 `{"v":1}` 推断 Int64，后续 `{"v":1.5}` 经 arrow-json 的 `parse::<i64>` 失败→`parse::<f64>`+`NumCast` 回退链**截断为 `1`**，无任何告警（vendored arrow-json 58.4.0 `primitive_array.rs:46-50` + num-traits `cast.rs:288-316`）。
2. **字段丢失**：首行没有的字段永远不成为列（arrow-json `reader/mod.rs:204`："Any columns not present in schema will be ignored"）。

暴露路径是所有**多消息合并解码**的调用方：`processor/json.rs:57`（`json_to_arrow` 整批 join 后解码——kafka→batch→json_to_arrow 常见链路）、`codec/json.rs:71,115`、`input/generate.rs:93`。输入 `{"v":1}`、`{"v":1.5}`、`{"v":2.9,"tag":"x"}` 的输出是单列 `v=[1,1,2]`、`tag` 消失——值静默错误、字段静默丢失。仓库自己的回归测试 `test_json_codec_skip_widens_types_like_fail_mode`（`codec/json.rs:361`）恰好因为 skip 与 fail **都截断**而通过，其"skip 拓宽类型"的文档声明名不副实。

## What Changes

- schema 推断改为扫描**全部**记录（`Some(1)` → `None`）：跨记录的 int/float 混见并集为 Float64（消除截断）、字段并集保留所有列（消除丢列）、null 出现的列放宽 nullable。
- 单消息解码路径（skip 模式逐条、Kafka 逐消息）语义不变（`Some(1)` 与 `None` 在单条输入上等价）。
- 修正 `test_json_codec_skip_widens_types_like_fail_mode` 的断言与文档声明，使其真正守护"skip 拓宽、fail 报错"的分化行为。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `codec-error-isolation`: ADDED requirement——JSON 解码的 schema 推断 SHALL 覆盖待解码批次的全量记录，跨记录类型并集取宽、字段取并集，禁止静默截断与静默丢列。

## Non-goals

- 不实现用户显式 schema 声明/覆写机制（既有 schema_registry/protobuf 显式路径不变）。
- 不改 `on_error: fail/skip` 的错误隔离语义本身（`codec-error-isolation` 既有 requirement 原样）。
- 不处理 Avro/Protobuf 解码（显式 schema，无推断问题）。
- 不为推断引入记录数上限（批大小已由通道/codec 批参数有界；上限会重新引入首行盲区）。

## Impact

- `crates/arkflow-plugin/src/component/json.rs`（唯一推断调用点）。
- 测试：`codec/json.rs`、`processor/json.rs` 新增截断/丢列回归；修正既有 skip-widens 测试。
- 性能：推断成本从 O(1) 变为 O(batch)，与解码同阶（批有界），无额外内存（`&[u8]` 上的 Cursor 流式扫描）。
- 文档：json codec 与 `json_to_arrow` 处理器页（en/zh）说明全量推断语义。
