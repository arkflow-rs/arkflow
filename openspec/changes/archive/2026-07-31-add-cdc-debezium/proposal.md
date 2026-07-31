## Why

ArkFlow 当前不具备任何 CDC（Change Data Capture）能力：`crates/arkflow-plugin/src/input/` 下的 14 个 input 均为定时轮询或推送式数据源（如 SQL input 走周期查询 `crates/arkflow-plugin/src/input/sql.rs`，并非事件驱动的变更捕获），目录中不存在 `cdc` 或 `debezium` 实现。这使得 ArkFlow 无法承接企业数据集成中最常见的「数据库变更实时同步」场景。

竞品 Benthos/Bento 虽已添加 CDC connector，但公开承认尚不成熟——端到端 exactly-once、schema 变更、DELETE 处理均存在局限（warpstreamlabs/bento#396）。ArkFlow 已具备零成本复用的基础：Kafka input 支持配置 codec 解码（`crates/arkflow-plugin/src/input/kafka.rs:62`、`kafka.rs:198-200` 的 `apply_codec_to_payload`），且其 offset 由 ack 显式推进（`kafka.rs:135-148`：`enable.auto.offset.store=false`，仅在 `KafkaAck::ack()` 中 `store_offset()`），与 `openspec/specs/input-durability` 的 ack-gated source-commit 语义一致。因此消费「Debezium → Kafka」的消息时，Kafka offset 天然即 CDC 位点——本次只需补齐 Debezium 事件的解析，即可最小侵入地获得覆盖 MySQL/PostgreSQL/MongoDB/SQLServer 等的 CDC 能力。

## What Changes

- 新增 `debezium_json` **codec**：将 Debezium Envelope（`before`/`after`/`op`/`source`/`ts_ms`）展平为 Arrow 列式 `MessageBatch`——输出操作类型列 `op`（`c`/`u`/`d`/`r`）、变更前/后数据、`source` 源元信息、`ts_ms` 时间戳，使下游 SQL processor 可直接按 `op` 与变更字段处理（如 `WHERE op IN ('c','u')` 取 upsert、`op='d'` 取删除）。
- 经 Kafka input 现有 codec 接入点（`kafka.rs:198-200`）零侵入接入：用户配置 `input.codec.type: debezium_json` 即可获得 CDC 流，无需新 input 类型。
- 位点管理复用现有机制、不新增：Kafka offset 经 ack-gated commit（`input-durability`）即 CDC 位点，保持 at-least-once。
- 配套 example 配置与组件文档。

## Non-goals

- 不实现 MySQL binlog / PostgreSQL 逻辑复制**直连**（零依赖 CDC）——留作未来独立 input。
- 不做 Schema Registry 集成与 schema 演进治理（方向② Change 2）。
- 不做端到端 exactly-once（方向② Change 3）；本次维持 at-least-once，重复由下游 sink 幂等吸收。
- 不支持 Debezium 的 Avro/Protobuf 序列化格式（先支持 JSON；Avro 依赖 Schema Registry，随 Change 2 落地）。

## Capabilities

### New Capabilities
- `debezium-cdc-parsing`: 将 Debezium Envelope JSON 解析为列式 Arrow（`op`/`before`/`after`/`source`/`ts_ms`），以 codec 形式暴露，复用 Kafka input 的 codec 接入与 ack-gated offset 提交。

### Modified Capabilities
<!-- CDC 消费是 input-durability 的新用例，不改变其现有 requirement；ack-gated source-commit 语义对 Kafka offset（即 CDC 位点）原样适用，故无 spec 级修改。 -->
（无）

## Impact

- 新增 `crates/arkflow-plugin/src/codec/debezium.rs`（codec 实现），并在 `crates/arkflow-plugin/src/codec/mod.rs:19` 的 `init()` 中注册 `register_codec_builder("debezium_json", ...)`。
- Envelope 解析复用现有 `component::json`（参考 `crates/arkflow-plugin/src/processor/json.rs:73` 对 `try_to_arrow` 的使用），无新第三方依赖。
- 接入点 `crates/arkflow-plugin/src/input/kafka.rs:198-200`（`apply_codec_to_payload`）无需改动。
- 新增 example `examples/cdc_debezium.yaml` 与组件文档 `docs/docs/components/`。
- 战略来源：`openspec/PLANNING.md` 方向② Change 1。
