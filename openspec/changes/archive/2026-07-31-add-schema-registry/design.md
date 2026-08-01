## Context

当前 protobuf codec 在 build 时从本地 `.proto` 构建固定 `MessageDescriptor`（`codec/protobuf.rs:68-88`），decode 用该固定 descriptor（`:97-118`），不支持多版本/动态 schema/Confluent wire format（见 `proposal.md`）。可复用基础已就位：

- `component::protobuf::protobuf_to_arrow`（`component/protobuf.rs:131`）——payload → Arrow 解码逻辑，可直接复用。
- `component::protobuf::parse_proto_file`（`:68`）——从**文件路径**解析；registry 返回 .proto **字符串**，需新增"从字符串解析"的能力。
- `reqwest` 已在 `arkflow-plugin/Cargo.toml:42`，无需新依赖做 HTTP。

## Goals / Non-Goals

**Goals:**
- 新增 `schema_registry` codec：Confluent wire format（`0x00` + schema id + payload）→ 按 id 从 Schema Registry 拉 Protobuf schema → 动态解码。
- 多版本 schema 天然支持（不同 id 用不同 descriptor）。
- 按 id 缓存 descriptor，避免每消息 HTTP。
- 复用 `component::protobuf` 解码；最小侵入现有 protobuf codec。

**Non-Goals:**
- Avro / JSON Schema 类型（先 Protobuf）。
- 显式兼容性检查（BACKWARD/FORWARD）——由 registry 端保证。
- 写入 registry（只读消费）。
- Protobuf schema references（import 其他 schema）——先单文件 schema。
- 修改现有 `protobuf` codec 行为。

## Decisions

### 决策 1：独立 `schema_registry` codec，而非扩展 protobuf codec
**选择**：新增独立 codec。

**Alternatives**：
- *扩展 protobuf codec 加 registry 模式*：`ProtobufCodec` 持有固定 descriptor，改动态需重构 decode（per-message 拉 schema），侵入大、违背 surgical。
- *新 processor*：wire format 解析 + schema 拉取是 input 解码职责，非 pipeline transform。

**理由**：wire format + HTTP + 动态 schema 是独立职责；独立 codec 零侵入现有 protobuf codec，复用 `component::protobuf` 解码。

### 决策 2：Confluent wire format
消息 = `[0x00][schema_id: 4 字节大端][payload]`。decode 剥前 5 字节，校验 magic=`0x00`，读 schema id，余为 payload。Confluent 是 Kafka 生态事实标准，不自造格式。

### 决策 3：`SchemaResolver` trait 抽象 + REST 实现 + 按 id 缓存
**选择**：定义 `SchemaResolver` trait（`fetch_schema(id) -> Protobuf schema 字符串`）。生产用 `RestSchemaResolver`（reqwest，`GET {registry}/schemas/ids/{id}`，可选 Basic/bearer auth）；测试用 `InMemorySchemaResolver`（无需 HTTP mock）。codec 持 `Arc<dyn SchemaResolver>` + 按 id 缓存 `MessageDescriptor`。

**Alternatives**：直接在 codec 内嵌 reqwest——可测性差（需 HTTP mock crate）。

**理由**：trait 抽象让 wire format + 缓存 + 多版本逻辑可纯单元测试（InMemoryResolver），REST 仅是 resolver 的一个实现。

### 决策 4：复用 `component::protobuf`，扩展 schema 来源
`protobuf_to_arrow`（`:131`）直接复用。新增 `component::protobuf::parse_proto_source(schema: &str) -> MessageDescriptor`（从 .proto 字符串解析，复用 `protobuf-parse`，已在依赖），供 registry schema 使用。`parse_proto_file`（文件路径）保持不变。

### 决策 5：多版本天然支持
不同 schema id → 不同 descriptor → 按 id 缓存。decode 用 per-message schema id 对应的 descriptor。无需显式版本管理；schema 演进产生新 id，旧 id descriptor 仍可用。

### 决策 6：依赖前置 change `refactor-codec-async`（Codec trait async 化）
schema_registry codec 的 `decode` 需经 reqwest **异步**拉取 schema，但现有 `Decoder` 是 sync trait（`core/codec/mod.rs`）。在 tokio runtime 内用 `reqwest::blocking` 会 panic；纯 sync HTTP（ureq）会阻塞 tokio worker（anti-pattern）。

**选择**：Codec trait async 化作为**独立前置 change** `refactor-codec-async`（见 `openspec/changes/refactor-codec-async/`），先于本 change 完成并归档。本 change 的 `RestSchemaResolver` 随之用 reqwest async（无阻塞），本身不再含 trait 重构（原 phase 0 已移出）。

**影响面**（grep 实测，2026-07-31，归 refactor-codec-async 承接）：trait 定义 + json/protobuf/debezium 3 个 codec impl + input/output `codec_helper` ~5 个包装 fn + ~15 个调用点加 `.await` ≈ 20-25 处机械编辑，**逻辑零变更**。

**Alternatives**：sync ureq（阻塞 worker + 新依赖）、`block_in_place`（局部技巧，每个 IO codec 重复）——均不如独立 async 重构干净。

**理由**：把无行为变更的重构独立成 change，可独立验证（全绿即无回归）、独立 review；本 change 聚焦 schema_registry 功能。

## Risks / Trade-offs

- **[首次 schema HTTP 延迟]** → 按 id 缓存（首次后零 HTTP）；预热为 Non-goal。
- **[registry 不可达]** → decode 返回 `Error`（明确失败，不静默）；本地 fallback 为 Non-goal。
- **[Protobuf schema references]** → 先 Non-goal（单文件 schema）；若实际常见再补。
- **[认证复杂度]** → 先 Basic + bearer；mTLS 等后续。
- **[schema 字符串解析与文件解析差异]** → 复用同一 `protobuf-parse`，行为一致；测试覆盖。

## Migration Plan

- 纯新增 codec + `component::protobuf` 加一个 pub fn，**无破坏性变更**。
- 部署：input/output 配 `codec: { type: "schema_registry", registry_url, message_type, ... }`。
- 回滚：移除 codec 配置或回退注册。

## Open Questions

- schema references（Confluent Protobuf 支持引用其他 schema）——先 Non-goal，实现时确认生产常见度。
- 缓存上限/淘汰——按 id 缓存（id 不可变），若 schema 版本极多需加上限（LRU）；先无界，按需加。
