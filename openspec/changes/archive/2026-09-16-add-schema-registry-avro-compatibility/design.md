## Context

`schema_registry` codec（`crates/arkflow-plugin/src/codec/schema_registry.rs`）当前结构：`SchemaResolver` trait（fetch schema string by id）→ `SchemaRegistryCodec`（wire format 解析 + `DashMap<u32, MessageDescriptor>` 缓存 + `protobuf_to_arrow`）→ `RestSchemaResolver`（Confluent REST，Basic/Bearer 认证，`schemaType` 非 PROTOBUF 即拒绝）。配置面：`registry_url`/`message_type`（必填）/`auth`。测试基础：`InMemorySchemaResolver` + wiremock 认证测试已有先例。

Codec trait（`arkflow-core/src/codec/mod.rs`）无异步 init/connect 钩子——所有网络行为都发生在 decode/encode 内，既有模式是按 id 惰性拉取。

## Goals / Non-Goals

**Goals:**
- Avro wire-format payload 解码为 Arrow batch（扁平映射，覆盖常用逻辑类型）。
- schemaType 驱动的 Protobuf/Avro 分派，`message_type` 降级为条件必填。
- subject 兼容级别门禁（fail-fast、结果缓存）。
- 文档/示例/生成物同步。

**Non-Goals:** 见 proposal（envelope 扁平化、编码方向、Apicurio/JSON Schema、嵌套结构映射、本地 schema 推导、EOS L3、原生 CDC）。

## Decisions

1. **依赖选 `apache-avro`（官方 Rust 实现）**——纯解码器（`Reader`/`Schema::parse`/`Value`），无传输层负担。版本上 workspace 已有 `datafusion = { features = ["avro"] }`，实施时经 `cargo tree -i apache-avro` 核对是否与 datafusion 的传递依赖收敛到同一版本；不收敛则以直接依赖为准（Avro 解码自包含，重复编译仅影响构建时间）。
2. **分派依据 = registry 响应的 `schemaType`，而非配置项**——wire format 本身不携带类型信息，registry 是唯一权威；新增 `format` 配置属冗余且可能与 registry 不一致。`schemaType` 缺省按 PROTOBUF：Confluent API 语义上缺省为 AVRO，但本 codec 既有行为把缺省当 Protobuf 用（现网 protobuf 用户可能正依赖），改变缺省会破坏兼容——保持现行为并在 spec 显式声明。
3. **缓存条目改枚举 `ResolvedSchema { Protobuf(MessageDescriptor), Avro(Schema) }`**——同一 codec 实例天然支持同流混布两种格式 id（与现有多版本解码语义一致）；decode 时对空 batch/合并逻辑复用现有 `concat_batches` 路径。
4. **Avro→Arrow 映射独立成 `codec/avro_arrow.rs` 模块**——镜像 `protobuf_to_arrow` 的「顶层字段→列、单行 batch、不支持类型报错」约定，便于两类 codec 行为对照；映射函数纯同步、无 IO，可直接单测。`[null, T]` union 解包为 nullable T（Avro 数据最常见的可空写法）；多分支 union 在 v1 报错（Arrow 无直接对应，静默取首分支会丢数据）。
5. **兼容性门禁 = 读 subject 配置的级别并按秩比较，不做本地 schema 推导**——registry 是兼容性的权威（尤其 Avro 兼容规则复杂，Rust 生态无成熟兼容校验实现）；`GET /config/{subject}?defaultToGlobal=true` 返回 subject 级别或全局缺省，秩映射 NONE=0 < BACKWARD/FORWARD=1 < FULL=2（`_TRANSITIVE` 与基础同级——transitive 更严格，与基础级别满足同样的最低秩要求）。用户语义：`min_compatibility` 声明「本管道要求该 subject 的注册兼容纪律不低于此级别」，级别劣化（如被运维改为 NONE）在第一条消息即暴露，而非静默吞下不兼容的新版本。
6. **门禁在首次 decode 时惰性执行，`tokio::sync::OnceCell` 缓存结果**——Codec trait 无异步 init 钩子，与按 id 惰性拉取同模式；一次请求、进程内缓存，开销可忽略。失败结果同样缓存（避免坏 subject 每条消息打一次 registry）。
7. **`message_type` 改可选的报错时机 = 解码到 Protobuf id 且未配置时**——build 期不知道数据是哪种格式（首条消息前无法判别），提前强制会把 Avro 用户挡在门外；错误信息指明「该 schema id 为 Protobuf，需配置 message_type」。

## Risks / Trade-offs

- [schemaType 缺省视为 PROTOBUF 与 Confluent 官方缺省（AVRO）不一致] → 属兼容性权衡（决策 2）；真实 registry 对 Avro subject 显式返回 `schemaType: AVRO`，缺省字段主要出现在极老版本 registry，已在 spec 中声明该约定。
- [Avro 解码按 writer schema（无 reader schema resolution）] → 与 Protobuf 路径对等（descriptor 即 writer schema）；跨版本字段增减时 batch schema 随 id 变化，`concat_batches` 对不一致 schema 报错——为既有行为，不引入新语义。
- [门禁秩模型把 BACKWARD 与 FORWARD 视为同级] → 两者代表不同的升级顺序承诺，本门禁只治理「纪律下限」而非方向；严格方向性校验列为 future（需结合消费者角色建模）。
- [`Decimal` 超出 decimal128 精度（p>38）] → 返回明确错误，不静默截断。

## Migration Plan

纯增量：新字段有缺省、缺省行为与现状一致。`message_type` 由必填转可选对既有配置无影响（字段仍被解析使用）。无部署顺序约束；回滚即还原代码与文档。

## Open Questions

无。
