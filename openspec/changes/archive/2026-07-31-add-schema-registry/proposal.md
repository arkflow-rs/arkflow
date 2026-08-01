## Why

ArkFlow 的 protobuf-codec 仅支持本地 `.proto` 文件、在 build 时固定**单一** message descriptor（`crates/arkflow-plugin/src/codec/protobuf.rs:42-49` 的 `ProtobufCodecConfig`、`:68-88` 的 `new()` 从 `parse_proto_file` 构建固定 `MessageDescriptor`、`:97-118` 的 `decode` 用该固定 descriptor）。这使其无法处理：

1. **多 schema 版本**——消息可能由不同版本的序列化器产生（schema 演进），固定 descriptor 无法适配；
2. **Confluent wire format**——Kafka 生态的事实标准：`0x00` magic byte + 4 字节 schema id + payload（`protobuf.rs:97-118` 当前直接把整个字节当 payload，不识别 schema id 前缀）；
3. **动态 schema 获取**——需运行时从 Schema Registry 按 id 拉取，而非 build 时读文件。

CDC 场景（Change 1 `add-cdc-debezium`）尤其需要：源表 schema 演进（加列/改类型）时，生产者按新 schema 注册到 Schema Registry，消费者必须按 schema id 解码——Benthos 的 CDC 也受 schema 变更困扰（warpstreamlabs/bento#396）。无 Schema Registry 集成，ArkFlow 无法接入 Confluent / Debezium+Schema Registry 企业生态。

## What Changes

- 新增 `schema_registry` codec：按 **Confluent wire format** 解析消息（剥 `0x00` + schema id + payload），按 schema id 经 **Confluent Schema Registry REST API**（`GET /schemas/ids/{id}`）拉取 Protobuf schema（带按 id 缓存，避免每消息 HTTP），动态构建 `MessageDescriptor` 解码 payload——天然支持多版本。
- 复用 `component::protobuf` 的 `protobuf_to_arrow` 解码逻辑；按需扩展 schema 来源（从 registry 返回的 schema 字符串解析 proto，而非仅本地文件）。
- codec 配置：registry URL、可选认证、message type、缓存策略。
- 配套 example 与组件文档。

## Non-goals

- **Avro / JSON Schema** 类型（先 Protobuf；Avro registry 后续）。
- **显式兼容性检查**（BACKWARD/FORWARD 校验工具）——由 Schema Registry 端配置保证，本 change 只做消费端按 id 解码。
- **写入 registry**（注册新 schema）——先只读消费。
- **Schema Registry 服务端实现**——只做 client。
- 修改现有 `protobuf` codec 行为（新 codec 独立，`protobuf-codec` spec 不变）。

## Capabilities

### New Capabilities
- `schema-registry-integration`: 按 Confluent wire format 的 schema id 从 Schema Registry 动态获取 Protobuf schema 并解码，支持多版本 schema 演进。

### Modified Capabilities
<!-- 新 codec 独立；component::protobuf 内部按需扩展 schema 来源属实现细节，不改变 protobuf-codec 的 spec 行为。 -->
（无）

## Impact

- 新增 `crates/arkflow-plugin/src/codec/schema_registry.rs`（codec + Schema Registry HTTP client + schema 缓存 + wire format 解析）。
- 可能扩展 `crates/arkflow-plugin/src/component/protobuf.rs`（从 registry schema 字符串解析 proto，复用 `protobuf_to_arrow`）。
- 注册于 `crates/arkflow-plugin/src/codec/mod.rs` 的 `init()`。
- 依赖：`reqwest`（已在 workspace deps）；Confluent REST client 手写（无新重型 crate）。
- 新增 `examples/schema_registry.yaml` 与 `docs/docs/components/5-codecs/schema-registry.md`。
- 战略来源：`openspec/PLANNING.md` 方向② Change 2；与 Change 1（CDC）协同。
