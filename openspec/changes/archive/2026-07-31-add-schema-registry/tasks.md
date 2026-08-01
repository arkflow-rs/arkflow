## 1. component::protobuf 扩展（schema 来源）

- [x] 1.1 新增 `pub fn parse_proto_source(schema: &str, message_type: &str) -> Result<MessageDescriptor, Error>`（从 .proto 字符串解析，复用 `protobuf-parse`，参考现有 `parse_proto_file` `component/protobuf.rs:68`）
- [x] 1.2 `parse_proto_source` 由 schema_registry 测试间接覆盖（decode_single / cache / multi-version 均经它解析）
- [x] 1.3 `cargo build --package arkflow-plugin` 通过（tempfile 已从 dev-deps 移到 dependencies）

## 2. schema_registry codec 核心

- [x] 2.1 创建 `crates/arkflow-plugin/src/codec/schema_registry.rs`，定义 `SchemaRegistryCodec`（持有 `Arc<dyn SchemaResolver>` + 按 id 的 `DashMap` descriptor 缓存 + message_type）
- [x] 2.2 定义 `SchemaResolver` trait（`async fn fetch_schema(&self, id: u32) -> Result<String, Error>`）
- [x] 2.3 Confluent wire format 解析（剥 `0x00` + 4 字节大端 schema id + payload，校验 magic 与长度）
- [x] 2.4 `RestSchemaResolver`（**reqwest async**，`GET {registry}/schemas/ids/{id}`，可选 Basic/bearer auth；依赖 refactor 的 async Codec）
- [x] 2.5 按 id 缓存 descriptor（`DashMap`；命中跳过 HTTP）
- [x] 2.6 `decode`：wire format → id → resolver 拉/缓存 schema → `parse_proto_source` → `protobuf_to_arrow` 解码
- [x] 2.7 `cargo build --package arkflow-plugin` 通过

## 3. 注册与元数据

- [x] 3.1 `schema_registry.rs` 的 `init()`：`register_codec_builder("schema_registry", ...)` + `ComponentMetadata::with_schema`（registry_url / message_type / auth）
- [x] 3.2 `crates/arkflow-plugin/src/codec/mod.rs` 的 `init()` 追加 `schema_registry::init()?;`
- [x] 3.3 `./target/debug/arkflow components show codec schema_registry` 正确输出（描述/kind/schema）

## 4. 测试

- [x] 4.1 单元测试：wire format 解析（有效 / 非法 magic / 过短）—— `InMemorySchemaResolver`
- [x] 4.2 单元测试：缓存命中（同 id 多消息只 resolve 一次）—— `fetch_count` 断言
- [x] 4.3 单元测试：多版本（同 batch id=1 与 id=2 各自正确解码，fetch_count=2）
- [x] 4.4 单元测试：registry 错误（resolver 返回 Err）→ decode 返回 Err
- [x] 4.5 `cargo test --package arkflow-plugin codec::schema_registry` 全绿（7 passed）
- [x] 4.6 `cargo test --workspace --lib` 全绿（core 142 + plugin 217，无回归）
- [x] 4.7 RestSchemaResolver 认证 HTTP mock 测试（Basic/Bearer → Authorization 头，wiremock）—— 9 passed

## 5. Example、文档与门禁

- [x] 5.1 新增 `examples/schema_registry.yaml`
- [x] 5.2 新增 `docs/docs/components/5-codecs/schema-registry.md`
- [x] 5.3 `./target/debug/arkflow --config examples/schema_registry.yaml --validate` EXIT=0
- [x] 5.4 `cargo clippy --package arkflow-plugin` 对 schema_registry 无告警
- [x] 5.5 `cargo build --release --package arkflow` 通过（Finished，19m）
- [x] 5.6 CLAUDE.md codec 清单补 `Schema Registry`
