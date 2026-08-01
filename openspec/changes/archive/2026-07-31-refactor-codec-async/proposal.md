## Why

未来 IO 类 codec（首当其冲是 `add-schema-registry` 的 schema_registry codec）需要在 `decode` 内做异步 IO（如 HTTP 查询 Schema Registry），但 `Encoder`/`Decoder` 是 **sync** trait（`crates/arkflow-core/src/codec/mod.rs:23-31`）。在 tokio runtime 内：用 `reqwest::blocking` 会 panic（"Cannot start a runtime from within a runtime"）；用纯 sync HTTP（ureq）会阻塞 tokio worker thread（anti-pattern）。因此需先把 Codec trait async 化，作为 IO 类 codec 的前置基础。

## What Changes

- `Encoder`/`Decoder`（及组合 trait `Codec`）的 `encode`/`decode` 改为 `async`（`#[async_trait]`）。
- json / protobuf / debezium 三个 codec 实现的 `encode`/`decode` 改 `async`（**逻辑零变更**）。
- `input/codec_helper.rs` 的 `apply_codec_to_payload(s)`、`output/codec_helper.rs` 的 encode 包装改 `async fn`。
- 所有调用点加 `.await`：input（mqtt/nats/generate/memory/pulsar/websocket/redis/http）、output（sql/influxdb 等）、`memory.rs`、`temporary/redis.rs`、`buffer/join.rs`。
- **纯重构，无行为变更。**

## Non-goals

- 不改变任何 codec 的编解码行为或语义。
- 不新增 codec、不引入 IO（schema_registry 在 `add-schema-registry`）。
- 不改 input/output/buffer 的其他 trait。

## Capabilities

### New Capabilities
- `async-codec-contract`: Codec 的 `encode`/`decode` 为 `async`，使 codec 实现可执行异步 IO；现有 codec 行为不变。

### Modified Capabilities
<!-- 纯重构，json/protobuf/debezium 的编解码行为不变，故无 spec 级修改。 -->
（无）

## Impact

- `crates/arkflow-core/src/codec/mod.rs`（trait）
- `crates/arkflow-plugin/src/codec/{json,protobuf,debezium}.rs`（impl）
- `crates/arkflow-plugin/src/input/codec_helper.rs`、`output/codec_helper.rs`（包装 fn）
- 调用点：`input/{mqtt,nats,generate,memory,pulsar,websocket,redis,http}.rs`、`output/{sql,influxdb}.rs`、`memory.rs`、`temporary/redis.rs`、`buffer/join.rs`
- 依赖：`async-trait`（已在 workspace deps）
- 前置于 `add-schema-registry`
