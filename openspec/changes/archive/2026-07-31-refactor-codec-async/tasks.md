## 1. Codec trait async 化

- [x] 1.1 `crates/arkflow-core/src/codec/mod.rs`：`Encoder`/`Decoder` 加 `#[async_trait]`，`encode`/`decode` 改 `async fn`（`Codec` 组合 trait 不变）
- [x] 1.2 `cargo build --package arkflow-core` 通过

## 2. codec 实现 async 化（逻辑不变）

- [x] 2.1 `codec/json.rs`：`encode`/`decode` 改 `async`
- [x] 2.2 `codec/protobuf.rs`：`encode`/`decode` 改 `async`
- [x] 2.3 `codec/debezium.rs`：`encode`/`decode` 改 `async`
- [x] 2.4 `cargo build --package arkflow-plugin` 通过

## 3. codec_helper 包装 async 化

- [x] 3.1 `input/codec_helper.rs`：`apply_codec_to_payload` / `apply_codec_to_payloads` 改 `async fn`
- [x] 3.2 `output/codec_helper.rs`：`apply_codec_encode` 改 `async fn`

## 4. 调用点加 `.await`

- [x] 4.1 input：mqtt / nats / pulsar / generate / memory(:87) / websocket / http / redis / kafka 的 codec 调用加 `.await`
- [x] 4.2 output：sql / influxdb / kafka / mqtt / nats / pulsar / http / stdout / redis 加 `.await`
- [x] 4.3 `buffer/join.rs`、`temporary/redis.rs` 的 codec 调用加 `.await`
- [x] 4.4 `memory.rs:62`（sync `new` 内）：用 `block_in_place` + `Handle::block_on`（构建时 decode initial messages，保持行为；现有 test 均 codec=None 不触发）
- [x] 4.5 codec 测试同步 async 化（json/protobuf/debezium 的 `#[tokio::test]` + `.await`）
- [x] 4.6 `cargo build --workspace` 通过

## 5. 回归验证

- [x] 5.1 `cargo test --workspace --lib` 全绿（core 142 + plugin 210，无回归）
- [x] 5.2 `cargo clippy --package arkflow-core --package arkflow-plugin` 无 error
- [x] 5.3 `--validate examples/cdc_debezium.yaml` EXIT=0
