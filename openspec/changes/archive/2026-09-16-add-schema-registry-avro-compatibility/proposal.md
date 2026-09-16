## Why

`schema_registry` codec 的能力窄于方向② Change 2 的原定范围，生产使用有两个硬缺口：

1. **仅支持 Protobuf**：`RestSchemaResolver::fetch_schema`（`crates/arkflow-plugin/src/codec/schema_registry.rs:189-196`）对 registry 返回的 `schemaType` 非 `PROTOBUF` 即报错 "Unsupported schema type"。Avro 是 Confluent Schema Registry 生态最常用的格式（registry API 缺省 schemaType 即 AVRO），CDC（Debezium Avro）与企业现有 topic 大量以 Avro 形态存在——当前无法接入。
2. **兼容性治理从未交付**：Change 2 原定范围含「兼容性检查（向后/向前兼容策略）」（见 `openspec/changes/archive/2026-07-31-add-schema-registry/proposal.md` 关联的 PLANNING.md 方向② Change 2），实际仅交付了按 id 解码；文档页 `docs/docs/components/5-codecs/schema-registry.md:86` 明确写着 "It does not explicitly validate BACKWARD/FORWARD compatibility (enforced by the registry)"——下游消费者对 subject 兼容级别劣化（如被改为 NONE）毫无防备，数据契约可能静默破坏。

另有文档失真一并修正：`examples/howto_cdc_schema_registry.yaml:5-7` 宣称 schema_registry codec 下 "Envelope fields land on the batch exactly as with debezium_json"，但 `SchemaRegistryCodec::decode` 只做 `protobuf_to_arrow`，无 Debezium envelope 扁平化逻辑。

## What Changes

- **Avro 解码**：workspace 新增 `apache-avro` 依赖；`RestSchemaResolver` 按响应 `schemaType` 分派 `PROTOBUF`/`AVRO`（缺省视为 PROTOBUF，保持现行为）；codec 缓存条目改为枚举 `ResolvedSchema { Protobuf(MessageDescriptor), Avro(Schema) }`，decode 按缓存类型分派；新增 Avro 值→Arrow 列式映射（扁平约定，镜像 `protobuf_to_arrow`，含常见逻辑类型与 `[null, T]` union）；`message_type` 配置改为可选（仅 protobuf id 实际需要）。
- **主题兼容性门禁**：新增可选配置 `subject` 与 `min_compatibility`（`none|backward|forward|full`）；配置后首次 decode 惰性调用 `GET {registry}/config/{subject}?defaultToGlobal=true`，校验 subject 兼容级别达到配置的最低秩，否则报错 fail-fast。
- **文档与示例**：更新 `docs/docs/components/5-codecs/schema-registry.md`（Avro + 门禁 + 改写 Non-goals）；修正 `examples/howto_cdc_schema_registry.yaml` 的 envelope 扁平化失实注释；新增 `examples/schema_registry_avro.yaml` 并注册 example manifest；重新生成 inventory/config-schema；README codec 清单保持 parity。

## Capabilities

### New Capabilities

<!-- 无新增 capability：全部行为落在既有 schema-registry-integration 内。 -->

### Modified Capabilities

- `schema-registry-integration`：
  - 「按 schema id 从 Schema Registry 获取 schema」扩展为按 `schemaType` 分派 Protobuf/Avro 解码；
  - 「作为 codec 注册并可配置」中 `message_type` 从必填改为条件必填；
  - 新增「Avro 解码映射」与「主题兼容性门禁」两组 Requirement。

## Impact

- 代码：`crates/arkflow-plugin/src/codec/schema_registry.rs`（resolver 分派、缓存枚举、配置、门禁）、新增 `crates/arkflow-plugin/src/codec/avro_arrow.rs`、`crates/arkflow-plugin/src/codec/mod.rs`（模块声明）。
- 依赖：workspace 新增 `apache-avro`（实施时经 `cargo tree` 核对与 datafusion avro feature 的版本收敛，避免重复编译）。
- 配置兼容性：`message_type` 由必填改可选——现有 protobuf 配置继续工作（字段仍在、继续使用）；新增字段均有缺省，零破坏。
- 文档：`docs/docs/components/5-codecs/schema-registry.md`、`examples/schema_registry_avro.yaml`、`docs/reference/example-manifest.json`、生成物 inventory/config-schema。

## Non-goals

- Debezium Avro Envelope 的扁平化（codec 仍按 schema 原样映射，CDC envelope 扁平化若需要另行立项）。
- wire format 编码方向 / schema 注册（codec 维持解码侧 + JSON 编码兜底）。
- Apicurio Registry、JSON Schema 类型。
- 嵌套 Avro record/array/map 到 Arrow struct/list 的映射（与 `protobuf_to_arrow` 同策略：报错，保持扁平约定）。
- 本地 schema 兼容性推导（Avro reader/writer schema resolution、`POST /compatibility` 校验）——门禁只读 registry 的 subject 配置。
- EOS L3、原生 CDC 直连 input。
