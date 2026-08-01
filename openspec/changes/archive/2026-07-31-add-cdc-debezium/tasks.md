## 1. Codec 核心实现

- [x] 1.1 创建 `crates/arkflow-plugin/src/codec/debezium.rs`，定义 `DebeziumJsonCodec` 并实现 `Decoder`（参考 `codec/json.rs` 的结构）
- [x] 1.2 实现 Envelope 解析：把 `after` 字段扁平化为 Arrow 顶层列（复用 `component::json::try_to_arrow`，参考 `processor/json.rs:73`）
- [x] 1.3 附加顶层元列 `op`、`ts_ms`，以及 `source_db`/`source_table` 顶层列 + 完整 `source`（JSON 文本列）
- [x] 1.4 `before` 作为 JSON 文本列保留；`op="d"` 时顶层业务列取自 `before`
- [x] 1.5 字段缺失容错：`before`/`after`/`source` 缺失或为 null 时以 null/`"null"` 填充，不报错
- [x] 1.6 `cargo build --package arkflow-plugin` 通过

## 2. 注册与元数据

- [x] 2.1 在 `codec/debezium.rs` 实现 `pub(crate) fn init()`，调用 `register_codec_builder("debezium_json", Arc::new(DebeziumJsonCodecBuilder))`（参考 `codec/json.rs:62-63`）
- [x] 2.2 在 `crates/arkflow-plugin/src/codec/mod.rs` 的 `init()` 中追加 `debezium::init()?;`
- [x] 2.3 按 codec 现有元数据注册模式注册 `ComponentMetadata::with_schema`（含 codec 配置 schema），供 `components list/show/schema` 发现
- [x] 2.4 `./target/debug/arkflow components show codec debezium_json` 正确输出 schema（kind/description/schema）

## 3. 测试

- [x] 3.1 单元测试：`op="c"/"u"/"d"/"r"` 四种事件解析正确（含 `op`/业务列/`source_db` 断言）
- [x] 3.2 单元测试：`op="d"`（`after=null`）顶层业务列取自 `before`、不报错
- [x] 3.3 单元测试：`before`/`source` 缺失时的容错（不报错）
- [x] 3.4 单元测试：`source_db`/`source_table`/`ts_ms`/`before`/`source`（JSON 文本）提取正确
- [x] 3.5 `cargo test --package arkflow-plugin codec::debezium` 全绿（9 passed）
- [x] 3.6 `cargo test --workspace --lib` 全绿（core 142 + plugin 209，无回归）

## 4. Example 与文档

- [x] 4.1 新增 `examples/cdc_debezium.yaml`：Kafka input + `codec: { type: debezium_json }` + SQL processor 按 `op` 路由
- [x] 4.2 新增 `docs/docs/components/5-codecs/`（`_category_.json` + `debezium.md`）：Envelope 字段说明、部署架构、输出 schema、交付语义
- [x] 4.3 `./target/debug/arkflow --config examples/cdc_debezium.yaml --validate` 通过（EXIT=0）

## 5. 质量门禁

- [x] 5.1 `cargo clippy --package arkflow-plugin` 对 `debezium.rs` 无告警（workspace 预存 warning 非本次引入）
- [x] 5.2 `cargo build --release --package arkflow` 通过（exit 0）
- [x] 5.3 CLAUDE.md codec 清单补 `Debezium`（line 89 + Codec Components 节）；README 无 codec 清单，不补
